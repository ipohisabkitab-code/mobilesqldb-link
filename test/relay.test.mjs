// Local integration tests for the IPO Archive phone relay Worker.
//
// Starts `wrangler dev --local` (no Cloudflare account/login needed), then
// drives it with plain HTTP POSTs (the "PHP" side) and a fake phone client
// built on Node's built-in WebSocket (the "phone" side). Uses only
// node:assert / node:test primitives -- no extra devDependency needed.
//
// Run with: npm test  (== node test/relay.test.mjs)

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { createHash, randomBytes } from "node:crypto";
import { writeFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const WORKER_DIR = path.resolve(__dirname, "..");
const PORT = 8799;
const BASE_URL = `http://127.0.0.1:${PORT}`;
const WS_URL = `ws://127.0.0.1:${PORT}/phone`;

const API_KEY = "test-key-0123456789";
// Same length as API_KEY (19 chars) but different bytes -- exercises the
// actual timingSafeEqual comparison rather than short-circuiting on the
// length check the way a different-length wrong key (e.g. "wrong-key") does.
const WRONG_KEY_SAME_LENGTH = "test-key-0123456780";
const REPLY_TIMEOUT_MS = 1500;
const DEV_VARS_PATH = path.join(WORKER_DIR, ".dev.vars");

// ---------------------------------------------------------------------------
// Test harness plumbing
// ---------------------------------------------------------------------------

/**
 * Always (re)writes .dev.vars for local testing -- not only when missing --
 * so the test harness never runs against a stale or hand-edited file.
 * LOCAL_DEV=1 gates the read-to-completion body drain in src/index.js that
 * works around a `wrangler dev --local`-only connection-corruption quirk;
 * it must never be set in a real deploy (wrangler.toml / `secret put` never
 * set it, and this variable exists only here).
 */
function ensureDevVars() {
  writeFileSync(
    DEV_VARS_PATH,
    `PHONE_API_KEY=${API_KEY}\nREPLY_TIMEOUT_MS=${REPLY_TIMEOUT_MS}\nLOCAL_DEV=1\n`,
    "utf8"
  );
  console.log(`(wrote ${DEV_VARS_PATH} for local testing)`);
}

async function waitForPort(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      // Any HTTP response (even a 401/405) means the dev server is up.
      await fetch(url, { method: "POST" });
      return;
    } catch (err) {
      lastError = err;
      await sleep(300);
    }
  }
  throw new Error(`Worker did not come up on ${url} within ${timeoutMs}ms: ${lastError}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Kills wrangler and every descendant it spawned (npx -> wrangler's node ->
 * two workerd.exe runtime processes). Synchronous and blocking on purpose:
 * this is called right before `process.exit()`, and a fire-and-forget kill
 * would otherwise race the process exit and leave workerd.exe orphaned.
 */
function killProcessTree(child) {
  if (!child) return;
  if (process.platform === "win32") {
    if (child.pid) {
      // /T kills the whole tree rooted at this pid (cmd.exe -> npx -> node -> workerd x2).
      spawnSync("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
    }
    // Belt-and-braces: taskkill /T can race a subprocess that only just
    // spawned. Walk the process table once more for any workerd.exe (or
    // wrangler node process) still bound to our port and stop it directly.
    try {
      const ps = spawnSync(
        "powershell",
        [
          "-NoProfile",
          "-Command",
          `Get-CimInstance Win32_Process -Filter "Name='workerd.exe'" | Where-Object { $_.CommandLine -like '*${PORT}*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }`,
        ],
        { stdio: "ignore" }
      );
      void ps;
    } catch (_err) {
      // best-effort only
    }
  } else {
    if (child.killed || child.exitCode !== null) return;
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch (_err) {
      child.kill("SIGKILL");
    }
  }
}

// ---------------------------------------------------------------------------
// Fake phone helper
// ---------------------------------------------------------------------------

/** Connects a fake phone WebSocket client with the given API key header. */
function connectPhone(apiKey) {
  const headers = apiKey == null ? {} : { "X-Api-Key": apiKey };
  return new WebSocket(WS_URL, { headers });
}

/** Waits for the socket to open, rejecting on error/close-before-open. */
function waitOpen(ws) {
  return new Promise((resolve, reject) => {
    ws.addEventListener("open", () => resolve(ws), { once: true });
    ws.addEventListener("error", (ev) => reject(new Error("ws error: " + (ev.message || ev.type))), {
      once: true,
    });
    ws.addEventListener(
      "close",
      (ev) => reject(new Error(`ws closed before open (code=${ev.code})`)),
      { once: true }
    );
  });
}

/** Waits for a rejected upgrade: resolves once the socket errors/closes without opening. */
function waitRejected(ws, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    let opened = false;
    const timer = setTimeout(() => {
      if (!opened) reject(new Error("expected rejection but socket neither opened nor closed"));
    }, timeoutMs);
    ws.addEventListener("open", () => {
      opened = true;
      clearTimeout(timer);
      reject(new Error("expected websocket upgrade to be rejected, but it opened"));
    });
    ws.addEventListener("error", () => {
      clearTimeout(timer);
      resolve();
    });
    ws.addEventListener("close", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

/** Registers an auto-responder: whenever a request envelope arrives, reply with replyBodyFor(requestId, parsedEnvelope). */
function autoRespond(ws, replyBodyFor) {
  ws.addEventListener("message", (ev) => {
    const envelope = JSON.parse(ev.data.toString());
    const replyBody = replyBodyFor(envelope);
    if (replyBody === undefined) return; // caller wants to not reply (yet)
    ws.send(JSON.stringify({ requestId: envelope.requestId, body: replyBody }));
  });
}

/** Resolves with the next parsed envelope this phone socket receives. */
function nextEnvelope(ws) {
  return new Promise((resolve) => {
    ws.addEventListener(
      "message",
      (ev) => resolve(JSON.parse(ev.data.toString())),
      { once: true }
    );
  });
}

/**
 * A raw HTTP GET with an `Upgrade` header, without ever performing a real
 * WebSocket handshake. `fetch()` (undici) refuses to send `Upgrade` at all
 * ("invalid upgrade header" -- it is a forbidden/reserved header under the
 * Fetch spec), so this uses `node:http` directly to reach the Worker's
 * ordinary (non-101) HTTP response path -- e.g. a 401 from the auth check
 * that runs before the Upgrade header is even looked at.
 */
function rawUpgradeGet(pathname, headers) {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: "127.0.0.1", port: PORT, path: pathname, method: "GET", headers },
      (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => resolve({ status: res.statusCode, body }));
      }
    );
    req.on("error", reject);
    // In case the server ever DID upgrade, don't hang the test -- resolve
    // with the informational status and tear the socket down.
    req.on("upgrade", (res, socket) => {
      resolve({ status: res.statusCode, upgraded: true });
      socket.destroy();
    });
    req.end();
  });
}

function echoReplyBody(message) {
  return JSON.stringify({ status: "ok", rows: [], affected: 0, tables: [], message });
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

const results = [];

async function runCase(name, fn) {
  const start = Date.now();
  try {
    await fn();
    results.push({ name, pass: true, ms: Date.now() - start });
    console.log(`PASS  ${name} (${Date.now() - start}ms)`);
  } catch (err) {
    results.push({ name, pass: false, ms: Date.now() - start, error: err });
    console.log(`FAIL  ${name} (${Date.now() - start}ms)`);
    console.log(`      ${err.stack || err}`);
  }
}

async function main() {
  ensureDevVars();

  console.log(`Starting: npx wrangler@4.131.1 dev --local --port ${PORT}`);
  // All arguments here are fixed literals (no untrusted input), so building
  // one command string for `shell: true` on Windows (needed because `npx`
  // is a .cmd shim there) is safe.
  const wrangler =
    process.platform === "win32"
      ? spawn(`npx wrangler@4.131.1 dev --local --port ${PORT}`, {
          cwd: WORKER_DIR,
          stdio: ["ignore", "pipe", "pipe"],
          shell: true,
        })
      : spawn("npx", ["wrangler@4.131.1", "dev", "--local", "--port", String(PORT)], {
          cwd: WORKER_DIR,
          stdio: ["ignore", "pipe", "pipe"],
          // `detached: true` makes this child the leader of a new process
          // group (its pid doubles as the group id), so killProcessTree's
          // `process.kill(-child.pid, ...)` below actually reaches every
          // descendant it spawned (npx's shell -> wrangler's node -> the
          // workerd runtime) instead of only the immediate child. Without
          // this, non-Windows cleanup can leave workerd running.
          detached: true,
        });

  // Wrapped in an object (rather than a plain `let`) so it can be handed to
  // runTestCases() below and read there too -- the abort-mid-request case
  // inspects it for an unhandled exception logged around that point.
  const wranglerState = { text: "" };
  wrangler.stdout.on("data", (d) => (wranglerState.text += d.toString()));
  wrangler.stderr.on("data", (d) => (wranglerState.text += d.toString()));

  const exitedEarly = once(wrangler, "exit").then(([code]) => {
    throw new Error(`wrangler dev exited early with code ${code}\n${wranglerState.text}`);
  });

  try {
    await Promise.race([waitForPort(`${BASE_URL}/health`, 45000), exitedEarly]);
  } catch (err) {
    console.error(err.message || err);
    killProcessTree(wrangler);
    process.exit(1);
  }
  console.log(`Worker is up on ${BASE_URL}`);

  // Cleanup (killing wrangler and every descendant it spawned) must run no
  // matter how the cases below finish -- an exception escaping a case here
  // (none should, runCase catches its own) must never leave workerd.exe
  // running.
  try {
    await runTestCases(wranglerState);
  } finally {
    killProcessTree(wrangler);
  }

  const failed = results.filter((r) => !r.pass);
  console.log("");
  console.log(`${results.length - failed.length}/${results.length} cases passed`);
  if (failed.length > 0) {
    console.log("Failed cases:");
    for (const f of failed) console.log(`  - ${f.name}`);
    process.exit(1);
  }
  process.exit(0);
}

async function runTestCases(wranglerState) {
  // --- a. missing / wrong API key -> 401 -------------------------------
  await runCase("a1: POST /query without key -> 401", async () => {
    const res = await fetch(`${BASE_URL}/query`, { method: "POST", body: "{}" });
    assert.equal(res.status, 401);
    const json = await res.json();
    assert.equal(json.status, "error");
  });

  await runCase("a2: POST /query with wrong key -> 401", async () => {
    const res = await fetch(`${BASE_URL}/query`, {
      method: "POST",
      headers: { "X-Api-Key": "wrong-key" },
      body: "{}",
    });
    assert.equal(res.status, 401);
  });

  await runCase("a3: POST /query with a same-length wrong key -> 401", async () => {
    // Same length as API_KEY -- must fall through to (and fail) the actual
    // byte comparison, not just the length check.
    assert.equal(WRONG_KEY_SAME_LENGTH.length, API_KEY.length);
    const res = await fetch(`${BASE_URL}/query`, {
      method: "POST",
      headers: { "X-Api-Key": WRONG_KEY_SAME_LENGTH },
      body: "{}",
    });
    assert.equal(res.status, 401);
  });

  // --- b. correct key, no phone -> 503 ----------------------------------
  await runCase('b: POST /query with key, no phone -> 503 "Phone is offline"', async () => {
    const res = await fetch(`${BASE_URL}/query`, {
      method: "POST",
      headers: { "X-Api-Key": API_KEY },
      body: "{}",
    });
    assert.equal(res.status, 503);
    const json = await res.json();
    assert.equal(json.status, "error");
    assert.equal(json.message, "Phone is offline");
    assert.equal(json.sent, false, "never reached a phone, so the website may send it again");
  });

  // --- c. /phone upgrade with wrong key -> rejected ---------------------
  await runCase("c: /phone upgrade with wrong key -> rejected", async () => {
    const ws = connectPhone("wrong-key");
    await waitRejected(ws);
  });

  await runCase("c2: /phone upgrade with a same-length wrong key -> rejected", async () => {
    assert.equal(WRONG_KEY_SAME_LENGTH.length, API_KEY.length);
    const ws = connectPhone(WRONG_KEY_SAME_LENGTH);
    await waitRejected(ws);
  });

  await runCase("c3: /phone upgrade with no key header at all -> rejected", async () => {
    const ws = connectPhone(null); // connectPhone omits the header entirely for null
    await waitRejected(ws);
  });

  // --- d. GET /query with key -> 405 ------------------------------------
  await runCase("d: GET /query with key -> 405", async () => {
    const res = await fetch(`${BASE_URL}/query`, {
      method: "GET",
      headers: { "X-Api-Key": API_KEY },
    });
    assert.equal(res.status, 405);
  });

  // --- e. round trip -----------------------------------------------------
  await runCase("e: round trip echo through the phone", async () => {
    const phone = connectPhone(API_KEY);
    await waitOpen(phone);
    try {
      autoRespond(phone, (envelope) => echoReplyBody("echo"));
      const res = await fetch(`${BASE_URL}/query`, {
        method: "POST",
        headers: { "X-Api-Key": API_KEY },
        body: JSON.stringify({ sql: "select 1" }),
      });
      assert.equal(res.status, 200);
      const text = await res.text();
      assert.equal(text, echoReplyBody("echo"));
    } finally {
      phone.close();
    }
  });

  // --- f. concurrent requests correlate correctly -------------------------
  await runCase(
    "f: two concurrent requests, matched by body content (not arrival order), replies sent in reverse",
    async () => {
      const phone = connectPhone(API_KEY);
      await waitOpen(phone);
      try {
        const seen = [];
        phone.addEventListener("message", (ev) => {
          const envelope = JSON.parse(ev.data.toString());
          seen.push(envelope);
        });

        const p1 = fetch(`${BASE_URL}/query`, {
          method: "POST",
          headers: { "X-Api-Key": API_KEY },
          body: JSON.stringify({ n: 1 }),
        });
        const p2 = fetch(`${BASE_URL}/query`, {
          method: "POST",
          headers: { "X-Api-Key": API_KEY },
          body: JSON.stringify({ n: 2 }),
        });

        // Wait until the phone has received both request envelopes.
        const deadline = Date.now() + 5000;
        while (seen.length < 2 && Date.now() < deadline) {
          await sleep(20);
        }
        assert.equal(seen.length, 2, "phone should have received 2 request envelopes");

        // Order independence: two concurrent fetches are not guaranteed to
        // reach the phone in the order they were issued, so identify each
        // envelope by its own body content (the `n` marker each POST sent),
        // never by its position in `seen`.
        const envelopeForN = (n) => {
          const found = seen.find((e) => JSON.parse(e.body).n === n);
          assert.ok(found, `no envelope carried body {"n":${n}}`);
          return found;
        };
        const env1 = envelopeForN(1);
        const env2 = envelopeForN(2);

        // Reply in the REVERSE of send order, keyed by each envelope's own requestId.
        phone.send(JSON.stringify({ requestId: env2.requestId, body: echoReplyBody("second") }));
        await sleep(50);
        phone.send(JSON.stringify({ requestId: env1.requestId, body: echoReplyBody("first") }));

        const [res1, res2] = await Promise.all([p1, p2]);
        const [body1, body2] = await Promise.all([res1.text(), res2.text()]);
        assert.equal(res1.status, 200);
        assert.equal(res2.status, 200);
        assert.equal(body1, echoReplyBody("first"), "p1's own response must carry the reply keyed to its own requestId");
        assert.equal(body2, echoReplyBody("second"), "p2's own response must carry the reply keyed to its own requestId");
      } finally {
        phone.close();
      }
    }
  );

  // --- g. phone never replies -> 504 after ~timeout -----------------------
  await runCase("g: phone never replies -> 504 after ~REPLY_TIMEOUT_MS", async () => {
    const phone = connectPhone(API_KEY);
    await waitOpen(phone);
    try {
      // No auto-responder registered -- the phone silently ignores every request.
      const start = Date.now();
      const res = await fetch(`${BASE_URL}/query`, {
        method: "POST",
        headers: { "X-Api-Key": API_KEY },
        body: "{}",
      });
      const elapsed = Date.now() - start;
      assert.equal(res.status, 504);
      const json = await res.json();
      assert.equal(json.message, "Phone did not respond in time");
      // Allow generous slack for CI/local scheduling jitter, but it must not
      // return instantly and must not wildly overshoot the configured timeout.
      assert.ok(elapsed >= REPLY_TIMEOUT_MS - 300, `elapsed ${elapsed}ms should be close to ${REPLY_TIMEOUT_MS}ms`);
      assert.ok(elapsed <= REPLY_TIMEOUT_MS + 5000, `elapsed ${elapsed}ms should not wildly exceed ${REPLY_TIMEOUT_MS}ms`);
    } finally {
      phone.close();
    }
  });

  // --- h. reconnect routes to the newest socket, immediately --------------
  await runCase("h: phone reconnect routes to the newest socket immediately", async () => {
    const phone1 = connectPhone(API_KEY);
    await waitOpen(phone1);
    autoRespond(phone1, () => echoReplyBody("phone1"));
    try {
      const phone2 = connectPhone(API_KEY);
      await waitOpen(phone2);
      autoRespond(phone2, () => echoReplyBody("phone2"));
      try {
        // Deliberately do NOT wait for phone1's close event -- under
        // `wrangler dev --local` that notification can take several
        // seconds to reach this client (a local-dev-only proxy delay), but
        // routing to the newest phone connection must be immediate and
        // must not depend on the old socket having already left OPEN.
        const res = await fetch(`${BASE_URL}/query`, {
          method: "POST",
          headers: { "X-Api-Key": API_KEY },
          body: "{}",
        });
        assert.equal(res.status, 200);
        const json = JSON.parse(await res.text());
        assert.equal(json.message, "phone2");
      } finally {
        phone2.close();
      }
    } finally {
      phone1.close();
    }
  });

  // --- i. large (~2MB) body passes through intact --------------------------
  await runCase("i: ~2MB POST body reaches the phone intact", async () => {
    const phone = connectPhone(API_KEY);
    await waitOpen(phone);
    try {
      const bigBody = randomBytes(2 * 1024 * 1024).toString("base64"); // ~2.7MB of text, well under 32MiB WS frame cap
      const expectedHash = createHash("sha256").update(bigBody).digest("hex");
      const expectedLength = bigBody.length;

      const envelopePromise = nextEnvelope(phone);
      const resPromise = fetch(`${BASE_URL}/query`, {
        method: "POST",
        headers: { "X-Api-Key": API_KEY },
        body: bigBody,
      });

      const envelope = await envelopePromise;
      assert.equal(envelope.body.length, expectedLength, "body length must survive the relay");
      const actualHash = createHash("sha256").update(envelope.body).digest("hex");
      assert.equal(actualHash, expectedHash, "body content must survive the relay byte-for-byte");

      phone.send(JSON.stringify({ requestId: envelope.requestId, body: echoReplyBody("big-ok") }));

      const res = await resPromise;
      assert.equal(res.status, 200);
      assert.equal(await res.text(), echoReplyBody("big-ok"));
    } finally {
      phone.close();
    }
  });

  // --- j. phone disconnects mid-request -> prompt 503 -----------------------
  await runCase("j: phone disconnects while request pending -> prompt 503", async () => {
    const phone = connectPhone(API_KEY);
    await waitOpen(phone);

    const envelopePromise = nextEnvelope(phone);
    const start = Date.now();
    const resPromise = fetch(`${BASE_URL}/query`, {
      method: "POST",
      headers: { "X-Api-Key": API_KEY },
      body: "{}",
    });

    await envelopePromise; // wait until the Worker has forwarded the request to the phone
    phone.close(); // simulate the phone dropping mid-request, without replying

    const res = await resPromise;
    const elapsed = Date.now() - start;
    assert.equal(res.status, 503);
    const json = await res.json();
    assert.equal(json.message, "Phone is offline");
    assert.equal(json.sent, undefined, "it reached the phone and may have run there: not marked as safe to resend");
    assert.ok(
      elapsed < REPLY_TIMEOUT_MS - 200,
      `disconnect should fail fast (${elapsed}ms), well before the ${REPLY_TIMEOUT_MS}ms timeout`
    );
  });

  // --- k. raw upgrade with no api key -> 401, never even considered as a
  // websocket upgrade ------------------------------------------------------
  await runCase("k: /phone upgrade attempt with no X-Api-Key -> 401 (edge auth, not 426)", async () => {
    const res = await rawUpgradeGet("/phone", { Upgrade: "websocket", Connection: "Upgrade" });
    assert.equal(res.status, 401);
    const json = JSON.parse(res.body);
    assert.equal(json.status, "error");
    assert.equal(json.message, "Invalid api key");
  });

  // --- l. reconnect: named close reason on the replaced socket, new phone
  // answers correlated by its own requestId ---------------------------------
  await runCase(
    "l: reconnect closes the old phone with code 1000 and the named reason; the new phone answers by requestId",
    async () => {
      const phone1 = connectPhone(API_KEY);
      await waitOpen(phone1);
      const phone1Closed = new Promise((resolve) => {
        phone1.addEventListener("close", (ev) => resolve(ev), { once: true });
      });
      try {
        const phone2 = connectPhone(API_KEY);
        await waitOpen(phone2);
        try {
          const envelopePromise = nextEnvelope(phone2);
          const resPromise = fetch(`${BASE_URL}/query`, {
            method: "POST",
            headers: { "X-Api-Key": API_KEY },
            body: "{}",
          });

          const envelope = await envelopePromise;
          phone2.send(JSON.stringify({ requestId: envelope.requestId, body: echoReplyBody("phone2") }));

          const res = await resPromise;
          assert.equal(res.status, 200);
          const json = JSON.parse(await res.text());
          assert.equal(json.message, "phone2", "the query must be answered by phone2's own requestId");

          const closeEvent = await phone1Closed;
          assert.equal(closeEvent.code, 1000);
          assert.equal(closeEvent.reason, "Replaced by a newer connection");
        } finally {
          phone2.close();
        }
      } finally {
        phone1.close();
      }
    }
  );

  // --- m. non-string reply body -> 502 unreadable reply --------------------
  await runCase("m: W-02 phone reply with a non-string body -> 502 unreadable reply", async () => {
    const phone = connectPhone(API_KEY);
    await waitOpen(phone);
    try {
      const envelopePromise = nextEnvelope(phone);
      const resPromise = fetch(`${BASE_URL}/query`, {
        method: "POST",
        headers: { "X-Api-Key": API_KEY },
        body: "{}",
      });

      const envelope = await envelopePromise;
      // An object body, not a JSON string -- exactly the shape a phone bug
      // (forwarding a parsed object instead of the original JSON text)
      // would produce.
      phone.send(JSON.stringify({ requestId: envelope.requestId, body: {} }));

      const res = await resPromise;
      assert.equal(res.status, 502);
      const json = await res.json();
      assert.equal(json.status, "error");
      assert.equal(json.message, "Phone sent an unreadable reply");
    } finally {
      phone.close();
    }
  });

  // --- n. client aborts while waiting; a later reply for that requestId must
  // not throw or wedge the Durable Object for the next caller ---------------
  await runCase(
    "n: client aborts /query while waiting; a later phone reply for that requestId is ignored without throwing",
    async () => {
      const phone = connectPhone(API_KEY);
      await waitOpen(phone);
      try {
        const outputBefore = wranglerState.text.length;

        const envelopePromise = nextEnvelope(phone);
        const controller = new AbortController();
        const resPromise = fetch(`${BASE_URL}/query`, {
          method: "POST",
          headers: { "X-Api-Key": API_KEY },
          body: "{}",
          signal: controller.signal,
        });
        const abortedRejection = resPromise.then(
          () => {
            throw new Error("expected the aborted fetch to reject");
          },
          (err) => err
        );

        const envelope = await envelopePromise;
        controller.abort();
        const rejection = await abortedRejection;
        assert.equal(rejection.name, "AbortError", `expected AbortError, got ${rejection.name}: ${rejection.message}`);

        // A reply for the now-abandoned request must be silently ignored --
        // it must not throw inside the Durable Object and must not wedge it
        // for the next caller.
        phone.send(JSON.stringify({ requestId: envelope.requestId, body: echoReplyBody("late-for-aborted") }));
        await sleep(100);

        const newOutput = wranglerState.text.slice(outputBefore);
        assert.ok(
          !/error|exception/i.test(newOutput),
          `wrangler dev logged an error while handling the late reply for an aborted request:\n${newOutput}`
        );

        // Prove the Durable Object is still healthy: a completely
        // independent request right afterwards must still work normally.
        const envelope2Promise = nextEnvelope(phone);
        const res2Promise = fetch(`${BASE_URL}/query`, {
          method: "POST",
          headers: { "X-Api-Key": API_KEY },
          body: "{}",
        });
        const envelope2 = await envelope2Promise;
        phone.send(JSON.stringify({ requestId: envelope2.requestId, body: echoReplyBody("still-alive") }));
        const res2 = await res2Promise;
        assert.equal(res2.status, 200);
        const json2 = JSON.parse(await res2.text());
        assert.equal(json2.message, "still-alive");
      } finally {
        phone.close();
      }
    }
  );

  // --- o. edge auth only: a wrong-key /query never wakes the Durable Object -
  await runCase(
    "o: a wrong-key /query is refused at the top-level edge check and never reaches (or counts against) the Durable Object",
    async () => {
      const countUrl = `${BASE_URL}/__test/do-fetch-count`;
      const readCount = async () => {
        const res = await fetch(countUrl, { method: "GET", headers: { "X-Api-Key": API_KEY } });
        assert.equal(res.status, 200);
        const json = await res.json();
        return json.count;
      };

      const before = await readCount();

      const res = await fetch(`${BASE_URL}/query`, {
        method: "POST",
        headers: { "X-Api-Key": "wrong-key" },
        body: "{}",
      });
      assert.equal(res.status, 401);

      const after = await readCount();
      assert.equal(
        after,
        before,
        "a wrong-key /query must be refused at the top-level edge check and never wake the Durable Object"
      );
    }
  );
}

main().catch((err) => {
  console.error("Test harness crashed:", err);
  process.exit(1);
});
