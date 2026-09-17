# Module 2 — Customer B: schema variation via DDL-as-code + tool gating

**Goal:** deliver a customer whose data is **shaped differently** — no promotions,
plus an online e-commerce channel — so the schema diverges and one AI tool drops
out. **~75 min.**

**Teaches:** DDL-artifact schema-as-code (propagation *and* divergence across
catalogs) and **config-driven tool availability** (tools follow data).

> The lesson: the same repo produces a different schema for Customer B by
> applying a **divergent DDL overlay**, and the promo tool disappears purely
> through **config** — no code fork.

---

## 🥖 Data swimlane (Tashay's team)

1. **Apply the divergent schema** to `cerebro_b`. The `apply_ddl` job applies the
   base DDL then the Customer B overlay (`ddl/variants/customer_b/`), which omits
   `promotions`/`promo_performance` and adds `online_orders` + `omnichannel_sales`:
   ```bash
   databricks bundle run apply_ddl -t customer_b
   ```
2. **Seed + run the omnichannel pipeline** (`pipeline_variant=omnichannel`, `enable_promo=false`):
   ```bash
   databricks bundle run seed_data    -t customer_b   # adds online_orders, no promos
   databricks bundle run run_pipeline -t customer_b
   ```
3. **Show the schema now differs from Customer A:**
   ```sql
   SHOW TABLES IN cerebro_b.gold;         -- omnichannel_sales present; promo_performance ABSENT
   SHOW TABLES IN cerebro_a.gold;         -- promo_performance present; omnichannel_sales absent
   SELECT * FROM cerebro_b.gold.omnichannel_sales LIMIT 20;
   ```
   ✅ **Pass:** `cerebro_b` has `omnichannel_sales`, no promo tables; gold includes the Online channel.
4. **Register functions** — only `f_market_share` is created (`enable_promo=false` skips `f_promo_lift`):
   ```bash
   databricks bundle run register_functions -t customer_b
   ```
   ```sql
   SHOW USER FUNCTIONS IN cerebro_b.gold;   -- f_market_share only
   ```

## 🤖 AI swimlane (Hosein's team)

1. **Genie space** over `cerebro_b.gold` (omnichannel notes in `genie/genie_space.md`).
2. **Deploy the app** with the Customer B tool list (`src/app/config/customer_b.yaml` → no `get_promo_lift`):
   ```bash
   databricks bundle deploy -t customer_b
   ```
3. **Test the app:**
   - "market share for Muffins online vs in Grocery" → **get_market_share** / **query_genie** (sees online channel)
   - "did our last promo lift sales?" → the model **does not** have a promo tool; it should explain promo data isn't available for this customer and offer what it *can* answer.

   ✅ **Pass:** the tool list has no `get_promo_lift`; the app degrades gracefully; Genie answers omnichannel questions.

---

## ✅ Module 2 done when
`cerebro_b`'s schema visibly differs from `cerebro_a` (omnichannel, no promo),
`f_promo_lift` was never created, and the Customer B app runs with exactly two
tools and handles promo questions gracefully.

## Talking point
Schema divergence for a customer = a **versioned DDL overlay** applied to that
customer's catalog — auditable, repeatable, and CI-ready if they later add a
pipeline. Tool availability followed the data through **config**, not a code
branch. That's how you keep 1000 customers from becoming 1000 forks.
