# Module 3 — Customer C: code variation + Lakebase serving

**Goal:** deliver a customer with the **same schema as A** but **different pipeline
logic**, plus a **Lakebase**-powered serving feature the chat can read sub-second
and write to. **~90 min.**

**Teaches:** DABs **code parameterization** (vary logic without touching schema)
and **Lakebase synced tables + OLTP** write path.

> Contrast with Module 2: B changed the *schema*; C changes the *code* only. The
> DDL for C is byte-identical to A — only `pipeline_variant=altlogic` differs.

---

## 🥖 Data swimlane (Tashay's team)

1. **Apply the schema** to `cerebro_c` — **identical DDL to Customer A**:
   ```bash
   databricks bundle run apply_ddl -t customer_c
   ```
   (Confirm no overlay is applied — same tables as `cerebro_a`.)
2. **Seed + run the alt-logic pipeline** (`pipeline_variant=altlogic`):
   ```bash
   databricks bundle run seed_data    -t customer_c
   databricks bundle run run_pipeline -t customer_c
   ```
   The alt logic computes `market_share.share_pct` with a **trailing-4-week
   revenue-weighted** methodology (and a wider silver watermark), so the numbers
   differ from A even though the schema is the same.
3. **Show same schema, different numbers:**
   ```sql
   -- identical columns to cerebro_a.gold.market_share:
   DESCRIBE cerebro_c.gold.market_share;
   -- different share_pct for the same slice (methodology change):
   SELECT * FROM cerebro_c.gold.market_share WHERE category='Croissants' AND region='West';
   ```
   ✅ **Pass:** schema matches A; `share_pct` differs (trailing-4-week weighting).

## 🤖 AI swimlane (Hosein's team)

1. **Enable Lakebase serving** (`enable_lakebase_serving=true` for `customer_c`). Run the Lakebase setup:
   ```bash
   databricks bundle run setup_lakebase -t customer_c
   ```
   This: (a) creates chat-memory tables (owned by the app SP), (b) creates the
   **synced table** `market_share_snapshot` from Delta `cerebro_c.gold.market_share`
   (reverse-ETL → Lakebase Postgres for sub-second reads), and (c) creates the
   **OLTP** `watchlist` table.
2. **Deploy the app** with the Customer C tool list (`src/app/config/customer_c.yaml` → adds `get_top_movers` + `save_to_watchlist`):
   ```bash
   databricks bundle deploy -t customer_c
   ```
3. **Test the Lakebase feature:**
   - "what are the top movers in the West right now?" → **get_top_movers** reads the Lakebase synced snapshot (sub-second; note latency in the Logs).
   - "watch product P0012 for me and note it's for the Q4 review" → **save_to_watchlist** INSERTs into Lakebase OLTP.
   - Verify the write:
     ```sql
     -- via the Lakebase SQL editor / psql, or a read tool:
     SELECT * FROM watchlist ORDER BY created_at DESC LIMIT 5;
     ```

   ✅ **Pass:** top-movers served from Lakebase; a watchlist row was written and is readable.

---

## ✅ Module 3 done when
`cerebro_c` has A's schema but alt-logic numbers, the app serves top-movers from
the Lakebase synced snapshot sub-second, and "watch this product" persists a row
in the OLTP watchlist.

## Talking point
Same schema, different code, shipped by flipping **one bundle variable** — that's
how you run per-customer business-logic variants without schema sprawl. And
Lakebase gives the agent a low-latency serving layer + a transactional memory the
chat writes to — reverse-ETL and OLTP in one managed Postgres, per customer.
