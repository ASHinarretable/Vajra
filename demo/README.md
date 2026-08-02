# Vajra Demo — Free, Live Deployment

A trimmed, single-instance version of Vajra: generates UPI transactions,
runs them through the same validation + feature-engineering + 5-rule fraud
engine as the full platform, and serves a live dashboard. No Kafka/Redis/
Airflow/Spark here — those live in the main repo's `docker-compose.yml` for
the full architecture story.

**Cost: $0.** Two free accounts, ~10 minutes of clicking, no credit card.

---

## Step 1 — Free Postgres (Neon)

1. Go to **neon.tech** and sign up (GitHub login is fastest).
2. Create a new project (any name, e.g. `vajra-demo`).
3. On the project dashboard, copy the **connection string** — it looks like:
   ```
   postgresql://user:password@ep-xxxx.region.aws.neon.tech/neondb?sslmode=require
   ```
4. Keep this tab open — you'll paste this string into Render in Step 3.

*Why Neon:* genuinely free tier (no trial expiry), serverless — sleeps when idle and wakes instantly on the next query, 0.5GB storage (far more than this demo needs).

---

## Step 2 — Push this repo to GitHub

If you haven't already:
```bash
git add -A
git commit -m "..."
git push
```
Render deploys straight from your GitHub repo.

---

## Step 3 — Free hosting (Render)

1. Go to **render.com** and sign up (GitHub login is fastest — it also grants Render access to your repos).
2. Click **New +** → **Blueprint**.
3. Select your `vajra` repository. Render will detect `demo/render.yaml` automatically.
4. It shows one service: `vajra-demo`. Click into it and set the environment variable:
   - `DATABASE_URL` = the connection string you copied from Neon in Step 1.
5. Click **Apply** / **Create**. Render builds and deploys (~2 minutes).
6. You'll get a URL like `https://vajra-demo.onrender.com` — that's your live dashboard.

**No `render.yaml`/Blueprint option showing?** Deploy manually instead:
- New + → Web Service → connect the repo
- Root Directory: `demo`
- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Add the `DATABASE_URL` env var as above
- Plan: Free

---

## Step 4 — Verify it's alive

Visit your Render URL. Within ~10 seconds you should see numbers ticking up
(Total Transactions, Fraud Alerts, etc.) — the background loop starts
generating transactions the moment the service boots.

---

## Known limitation: free tier sleeps

Render's free web services spin down after **15 minutes with no HTTP traffic**,
and spin back up (~30–50s cold start) on the next visit. While asleep, the
transaction generator is also paused — nothing new gets created until someone
loads the page again.

This is normal and fine for a portfolio demo (recruiters visiting a link
trigger a wake-up, they wait a few seconds, dashboard loads). If you want it
to feel closer to always-on for free:

### Optional: keep it awake with a free uptime pinger

1. Go to **cron-job.org** (free, no card) and sign up.
2. Create a new cron job:
   - URL: `https://vajra-demo.onrender.com/health`
   - Interval: every 10 minutes
3. This counts as HTTP traffic, so Render never sees 15 idle minutes and the
   service — and the generator loop — effectively never sleeps.

(This is a widely used community pattern for free-tier demos. Render doesn't
promise it'll work forever, but it's harmless and commonly done.)

---

## What this demo intentionally leaves out

To fit a free, single-instance host:
- **No Kafka** — producer and consumer are merged into one in-process loop
- **No Redis** — feature state lives in memory (fine for 1 instance, not for horizontal scaling)
- **No Airflow/Spark** — no batch layer; everything is real-time only
- **No Metabase/Grafana/Prometheus** — one built-in HTML dashboard instead

All of that exists in the main repo (`docker-compose.yml`) for the full,
production-shaped architecture — this demo folder exists purely to get a free,
shareable live link.

---

## Updating the demo after code changes

Render auto-redeploys on every push to your connected branch. Just:
```bash
git add -A
git commit -m "update demo"
git push
```
