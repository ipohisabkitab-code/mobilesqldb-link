# MobileSqlDB — the link

This is the small free service that lets a website read and write the SQL databases
stored on **your own phone**, through the MobileSqlDB app.

It runs on **your** Cloudflare account, not anyone else's. It keeps no data: it takes a
request from your website, hands it to your phone over an open connection, and passes your
phone's answer back. Your data never leaves your phone.

## Set it up from your phone — no computer needed

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/ipohisabkitab-code/mobilesqldb-link)

Tap the button, sign in to Cloudflare (a free account is enough), and it deploys this
service into your account. You will be asked for one value:

| Name | What to paste |
| --- | --- |
| `PHONE_API_KEY` | The **phone link key** from the app — Setup step 3, or Settings › Change key › Copy |

When it finishes, Cloudflare shows you an address like
`https://mobilesqldb-link.your-name.workers.dev`. You need it in two places:

1. **In the app** — change `https://` to `wss://` and add `/phone` at the end:
   `wss://mobilesqldb-link.your-name.workers.dev/phone`
2. **On your website** — it posts to `https://mobilesqldb-link.your-name.workers.dev/query`
   with the header `X-Api-Key: <the same key>`.

That is the whole setup. If the key in the app, here, and on your website all match, the app
shows **Connected**.

## If you would rather use a computer

Everything the button does can be done from a terminal instead:

```bash
npm ci
npx wrangler@4.131.1 login
npx wrangler@4.131.1 deploy
npx wrangler@4.131.1 secret put PHONE_API_KEY
```

## What it does

| Path | Who calls it | What happens |
| --- | --- | --- |
| `POST /query` | your website | The JSON is passed to the phone; the phone's reply is the HTTP response. |
| `WSS /phone` | your phone | The app holds this open so requests can reach it. |
| `GET /health` | anyone | A liveness check. Used by the app's connection test. |

Every door is guarded by the same `PHONE_API_KEY`. The service never looks inside the JSON
it carries, and never stores it. Logs show a status and a duration — never your data.

## Limits on Cloudflare's free plan

100,000 requests a day for Workers and the same for Durable Objects, resetting at 00:00 UTC.
For scale: copying 1 crore rows at 1,000 rows a request is 10,000 requests. Your phone
answers one request at a time, and the link waits at most 15 seconds for each reply.

## Changing the key

Change it in all three places, in this order — the link stops working until they match, which
usually takes about a minute.

1. In the app: Settings › Change key › Create new key › Copy.
2. Here: Cloudflare dashboard › your Worker › Settings › Variables, or
   `npx wrangler@4.131.1 secret put PHONE_API_KEY`.
3. On your website, wherever it keeps the key.

**Never paste a real key into a chat, an email, a ticket, or a file you plan to share.**

## Testing it locally

`npm test` starts `wrangler dev --local` and drives the relay with a fake phone. It writes a
`.dev.vars` with a throwaway test key, so no Cloudflare account or login is needed.

## Keeping this in step

This repository is the public, deployable copy of the `worker/` folder in the MobileSqlDB app
repository. `src/index.js`, `wrangler.toml`, `package.json`, `package-lock.json` and `test/`
must stay byte-identical between the two; only this README and `.dev.vars.example` are
specific to this repository.
