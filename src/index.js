// One Durable Object instance ("phone-relay") holds the phone's WebSocket and
// matches each inbound HTTP request to the phone's next reply. The phone side
// (RelayService.kt) speaks the same JSON envelope PHP sends -- this Worker
// never parses request/response bodies, only relays raw text frames.
//
// Runs on the Cloudflare Workers FREE plan:
// - Durable Object uses the SQLite storage backend (new_sqlite_classes) so it
//   is allowed on the free plan.
// - Uses the WebSocket Hibernation API (ctx.acceptWebSocket) so the object
//   can be evicted from memory between requests instead of staying resident.
// - Never logs request or response bodies -- they contain PAN numbers.

import { DurableObject } from "cloudflare:workers";

// Close reason used when a reconnect replaces an existing phone socket.
// Named so the value is defined once and used consistently, and so the
// phone side (RelayService.kt) and this file's own tests can assert on it
// exactly, rather than duplicating the string.
//
// NOT exported: workerd treats every top-level export of the entry module
// as a service/handler binding (alongside the default export and the
// PhoneRelay Durable Object class named in wrangler.toml), and rejects a
// plain string export with "Incorrect type for map entry ... not of type
// 'function or ExportedHandler'" -- the Worker fails to boot at all. A
// module-scoped (unexported) constant avoids that entirely.
const REPLACED_REASON = "Replaced by a newer connection";

function jsonResponse(payload, status) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

/**
 * An early-return response (401/426/405/503) never processes the incoming
 * request body. In production this MUST NOT buffer it: bodies can be up to
 * 100MB on the free plan, and buffering a rejected body is exactly the DoS
 * vector an unauthenticated caller could use against the Worker (repeated
 * large keyless POSTs, each fully read into memory before being rejected).
 * So by default this only *cancels* the body stream, which discards it
 * without reading its bytes.
 *
 * Under `wrangler dev --local` specifically, cancelling without reading
 * corrupts the *next* request on the same keep-alive connection --
 * workerd's local ProxyWorker throws "Can't read from request stream after
 * response has been sent" and fails the following request with "Network
 * connection lost". That is a local-dev-proxy quirk, not production
 * behaviour, so the full read-to-completion drain that works around it is
 * gated behind `env.LOCAL_DEV` -- set only by the test harness's generated
 * `.dev.vars` (never in a real deploy; `wrangler secret put` /
 * `wrangler.toml` never set it). The discarded text is never parsed or
 * logged either way.
 */
async function drainBody(request, env) {
  if (!request.body) return;
  try {
    if (env && env.LOCAL_DEV) {
      await request.text(); // local-dev-only workaround, see above
    } else {
      await request.body.cancel(); // production: never buffer a rejected body
    }
  } catch (_err) {
    // already drained/cancelled/errored -- ignore
  }
}

/**
 * Constant-time comparison of the caller-supplied API key against the
 * configured secret. Returns false (never throws) when the header is
 * missing, the secret is unset/empty, or the lengths differ.
 */
function isAuthorized(request, env) {
  const provided = request.headers.get("X-Api-Key");
  const expected = env.PHONE_API_KEY;
  if (!provided || !expected) return false;

  const encoder = new TextEncoder();
  const a = encoder.encode(provided);
  const b = encoder.encode(expected);
  if (a.length !== b.length) return false;
  return crypto.subtle.timingSafeEqual(a, b);
}

export class PhoneRelay extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // requestId -> { resolve, timer, socket }. `socket` is the exact phone
    // WebSocket this request was sent to, so that closing an old,
    // already-replaced phone socket never fails a request already routed
    // to a newer one (see failPendingFor below). resolve receives one of:
    //   { kind: "reply", body: string }
    //   { kind: "timeout" }
    //   { kind: "offline" }
    this.pending = new Map();
  }

  async fetch(request) {
    const start = Date.now();
    const url = new URL(request.url);

    // 1. Constant-time API key check, before anything else, on every path.
    if (!isAuthorized(request, this.env)) {
      await drainBody(request, this.env);
      this.log(url.pathname, 401, Date.now() - start);
      return jsonResponse({ status: "error", message: "Invalid api key" }, 401);
    }

    // LOCAL_DEV-only introspection endpoint: reports how many times this
    // Durable Object's fetch() has actually run (the read itself is
    // returned before the counter below is bumped, so reading it never
    // inflates the count). Lets a test prove that a wrong-key /query is
    // refused by the top-level edge check in the default export below and
    // never reaches this object at all. Never present in production --
    // env.LOCAL_DEV is set only by the test harness's generated .dev.vars.
    if (this.env.LOCAL_DEV && url.pathname === "/__test/do-fetch-count") {
      const count = (await this.ctx.storage.get("doFetchCount")) || 0;
      return jsonResponse({ count }, 200);
    }
    if (this.env.LOCAL_DEV) {
      const count = (await this.ctx.storage.get("doFetchCount")) || 0;
      await this.ctx.storage.put("doFetchCount", count + 1);
    }

    // 2. /phone is the phone's WebSocket upgrade endpoint.
    if (url.pathname === "/phone") {
      if (request.headers.get("Upgrade") !== "websocket") {
        await drainBody(request, this.env);
        this.log(url.pathname, 426, Date.now() - start);
        return jsonResponse({ status: "error", message: "Expected websocket upgrade" }, 426);
      }

      // W-01: allocate the sequence number BEFORE closing any existing
      // phone socket. Strictly increasing, persisted in DO storage so it
      // survives hibernation eviction, stamped onto the socket via a
      // hibernatable serialized attachment (in-memory instance fields do
      // not survive eviction, but attachments do). currentPhoneSocket()
      // uses this to route to the newest connection explicitly, rather
      // than relying on the replaced socket having already left OPEN.
      // A storage failure here must never drop an already-connected phone:
      // if this throws, bail out with the existing socket(s) left open and
      // untouched, rather than closing them and then failing to open the
      // new one.
      let seq;
      try {
        seq = await this.nextPhoneSeq();
      } catch (_err) {
        await drainBody(request, this.env);
        this.log(url.pathname, 503, Date.now() - start);
        return jsonResponse({ status: "error", message: "Phone is offline" }, 503);
      }

      // A phone reconnect must never leave two phone sockets attached:
      // close every previously attached socket first, and fail anything
      // still pending on it *now* -- not wait for its close event (which
      // can be delayed several seconds, e.g. under local dev) or for its
      // own request timeout.
      for (const existing of this.ctx.getWebSockets()) {
        try {
          existing.close(1000, REPLACED_REASON);
        } catch (_err) {
          // already closing/closed -- ignore
        }
        this.failPendingFor(existing);
      }

      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.serializeAttachment({ seq });
      // Hibernation API: the runtime may evict this object from memory
      // between messages and re-instantiate it on the next event.
      this.ctx.acceptWebSocket(server);

      this.log(url.pathname, 101, Date.now() - start);
      return new Response(null, { status: 101, webSocket: client });
    }

    // 3. Everything else (/query, /batch, /tables, /health, ...) is PHP
    // calling in, and must be POST.
    if (request.method !== "POST") {
      await drainBody(request, this.env);
      this.log(url.pathname, 405, Date.now() - start);
      return jsonResponse({ status: "error", message: "Method not allowed" }, 405);
    }

    const phoneSocket = this.currentPhoneSocket();
    if (!phoneSocket) {
      await drainBody(request, this.env);
      this.log(url.pathname, 503, Date.now() - start);
      // sent:false -- the request never reached the phone, so the website may
      // safely send it again once the phone is back (usually within seconds).
      return jsonResponse({ status: "error", message: "Phone is offline", sent: false }, 503);
    }

    // Body is passed through as an opaque string -- never JSON.parse it here
    // (a large batch would burn the 10ms CPU budget).
    const body = await request.text();
    const requestId = crypto.randomUUID();
    const timeoutMs = Number(this.env.REPLY_TIMEOUT_MS) || 15000;

    const result = await new Promise((resolve) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(requestId)) {
          resolve({ kind: "timeout" });
        }
      }, timeoutMs);

      this.pending.set(requestId, { resolve, timer, socket: phoneSocket });

      try {
        phoneSocket.send(JSON.stringify({ requestId, body }));
      } catch (_err) {
        clearTimeout(timer);
        this.pending.delete(requestId);
        resolve({ kind: "offline", sent: false });
      }
    });

    const elapsed = Date.now() - start;
    if (result.kind === "timeout") {
      this.log(url.pathname, 504, elapsed, requestId);
      return jsonResponse({ status: "error", message: "Phone did not respond in time" }, 504);
    }
    if (result.kind === "offline") {
      this.log(url.pathname, 503, elapsed, requestId);
      // Only a request that was never handed to the socket says sent:false. One
      // that was sent and then lost its phone may already have run there.
      const reply = { status: "error", message: "Phone is offline" };
      if (result.sent === false) reply.sent = false;
      return jsonResponse(reply, 503);
    }
    if (result.kind === "badReply") {
      this.log(url.pathname, 502, elapsed, requestId);
      return jsonResponse({ status: "error", message: "Phone sent an unreadable reply" }, 502);
    }

    this.log(url.pathname, 200, elapsed, requestId);
    return new Response(result.body, { headers: { "content-type": "application/json" } });
  }

  /** Allocates the next strictly-increasing phone connection sequence number. */
  async nextPhoneSeq() {
    const current = (await this.ctx.storage.get("phoneSeq")) || 0;
    const next = current + 1;
    await this.ctx.storage.put("phoneSeq", next);
    return next;
  }

  /**
   * The OPEN phone socket with the highest connection sequence number --
   * i.e. the newest one -- explicitly, rather than relying on every
   * previously-replaced socket having already transitioned out of OPEN.
   */
  currentPhoneSocket() {
    let best = null;
    let bestSeq = -Infinity;
    for (const ws of this.ctx.getWebSockets()) {
      const isOpen = ws.readyState === WebSocket.READY_STATE_OPEN || ws.readyState === 1;
      if (!isOpen) continue;
      const attachment = ws.deserializeAttachment() || {};
      const seq = typeof attachment.seq === "number" ? attachment.seq : -1;
      if (seq > bestSeq) {
        bestSeq = seq;
        best = ws;
      }
    }
    return best;
  }

  /** Called by the runtime when a phone socket sends a text frame. */
  async webSocketMessage(ws, message) {
    let envelope;
    try {
      const text = typeof message === "string" ? message : new TextDecoder().decode(message);
      envelope = JSON.parse(text);
    } catch (_err) {
      return; // not valid JSON -- ignore
    }

    if (!envelope || typeof envelope.requestId !== "string") return;

    const entry = this.pending.get(envelope.requestId);
    if (!entry) return; // no pending request for this id -- ignore
    // A reply is only accepted from the exact socket the request was sent
    // to -- a stale/replaced socket delivering a message that happens to
    // reuse a live requestId must never resolve it.
    if (entry.socket !== ws) return;

    this.pending.delete(envelope.requestId);
    clearTimeout(entry.timer);

    // W-02: the phone must reply with the response body as a JSON string
    // (it never JSON.parses/re-serializes PHP's payload itself). Anything
    // else -- an object, a number, null -- would otherwise be forwarded
    // as-is and become a 200 with an empty or "[object Object]" body.
    if (typeof envelope.body !== "string") {
      entry.resolve({ kind: "badReply" });
      return;
    }

    entry.resolve({ kind: "reply", body: envelope.body });
  }

  /** Called by the runtime when a phone socket closes. */
  async webSocketClose(ws, _code, _reason, _wasClean) {
    this.failPendingFor(ws);
  }

  /** Called by the runtime on a socket error. */
  async webSocketError(ws, _error) {
    this.failPendingFor(ws);
  }

  /**
   * Fail every request that was actually sent to `ws`, immediately, instead
   * of waiting for its timeout. Scoped to `ws` (not "every pending
   * request") so that closing an old phone socket that a reconnect just
   * replaced never fails a request already routed to the new one.
   */
  failPendingFor(ws) {
    for (const [requestId, entry] of this.pending) {
      if (entry.socket !== ws) continue;
      clearTimeout(entry.timer);
      entry.resolve({ kind: "offline" });
      this.pending.delete(requestId);
    }
  }

  /** Log request id, HTTP status and elapsed ms only -- never bodies (PAN numbers). */
  log(path, status, ms, requestId) {
    console.log(
      `relay path=${path} status=${status} ms=${ms}` + (requestId ? ` requestId=${requestId}` : "")
    );
  }
}

export default {
  async fetch(request, env) {
    // Authenticate here, before the Durable Object is ever woken or
    // forwarded to: a keyless (or wrong-key) caller must never be able to
    // reach the single shared phone-relay object at all, let alone push a
    // large body into it. The DO repeats this exact check on every request
    // it does receive, as defense in depth.
    if (!isAuthorized(request, env)) {
      await drainBody(request, env);
      console.log(`relay path=${new URL(request.url).pathname} status=401 ms=0`);
      return jsonResponse({ status: "error", message: "Invalid api key" }, 401);
    }

    const id = env.PHONE_RELAY.idFromName("phone-relay");
    const stub = env.PHONE_RELAY.get(id);
    // W-03: a Durable Object fetch failure (e.g. the object cannot be
    // reached) would otherwise surface as Cloudflare's own non-JSON 500,
    // which the PHP side cannot parse as the usual {status,message} shape.
    try {
      return await stub.fetch(request);
    } catch (_err) {
      console.log(`relay path=${new URL(request.url).pathname} status=503 ms=0`);
      return jsonResponse({ status: "error", message: "Phone is offline" }, 503);
    }
  },
};
