# Genie Space — Cerebro Market Analytics (`${catalog}.gold`)

**From:** claude-code
**To:** genie-code
**Date:** 2026-09-07
**Priority:** high
**Status:** actionable

---

## Summary

Genie-as-code definition for the **Cerebro Market Analytics** space that the app
calls via `query_genie`. One space per customer catalog, built over
`${catalog}.gold`. This doc is the buildable contract: data sources, curated
instructions, trusted assets, ~10 sample questions, and certified benchmark Q&A.
Structure follows `cerebro-genie-best-practices.md` (§1b accuracy levers).

> **Terminology:** Genie Spaces are now **Genie Agents**; the REST API still uses
> `/genie/spaces/{id}`. `${var.genie_space_id}` is the id the app calls.

## Space configuration

- **Name:** `Cerebro Market Analytics — ${catalog}`
- **Warehouse:** a **Serverless** SQL warehouse (`${var.warehouse_id}`). Serverless
  is required for low latency — a cold Pro/Classic warehouse is the #1 cause of
  "simple questions are slow" (best-practices §1a lever 1).
- **Scope:** the `${catalog}.gold` schema ONLY. Genie reads gold; it never sees
  bronze/silver. Keep the object count low (≤5 tables) for speed + accuracy.

## Data sources (curated, ≤5 tables)

Attach these gold tables. Column names are the CONTRACT.md ground truth — do not
rename.

### `${catalog}.gold.sales_daily`  — daily sales fact (all customers)
| column | type | notes / synonyms |
|---|---|---|
| `sales_date` | DATE | "day", "date" |
| `product_id` | STRING | join key to products |
| `store_id` | STRING | join key to stores |
| `region` | STRING | "market", "area" — entity-match values (e.g. `NE`→"Northeast") |
| `category` | STRING | product category — "segment" |
| `brand` | STRING | "label" |
| `units` | BIGINT | "quantity", "volume sold" |
| `revenue` | DECIMAL(18,2) | "sales", "$", "turnover" |

### `${catalog}.gold.market_share` — brand share by period/region/category (all)
| column | type | notes |
|---|---|---|
| `period` | STRING | reporting period, e.g. `2026-Q2` — "quarter", "period" |
| `region` | STRING | |
| `category` | STRING | |
| `brand` | STRING | |
| `brand_revenue` | DECIMAL(18,2) | revenue for this brand in the cell |
| `category_revenue` | DECIMAL(18,2) | total category revenue in the cell |
| `share_pct` | DECIMAL(6,3) | `brand_revenue / category_revenue * 100`. **"market share".** |

> **Customer C (altlogic):** `share_pct` uses a **trailing-4-week revenue-weighted**
> methodology (not point-in-period). Same columns/DDL — only the number differs.
> Add a general-instruction note so Genie explains share as trailing-4-week for C.

### `${catalog}.gold.promo_performance` — promotion lift (Customers **A / C only**)
| column | type | notes |
|---|---|---|
| `promo_id` | STRING | |
| `product_id` | STRING | |
| `baseline_units` | BIGINT | units without the promo |
| `promo_units` | BIGINT | units during the promo |
| `lift_pct` | DECIMAL(6,2) | `(promo_units - baseline_units)/baseline_units*100`. "uplift". |

> **Customer B:** this table does **not** exist (no promotions data). Do NOT attach
> it to the B space, and don't add promo instructions/examples there.

### `${catalog}.gold.omnichannel_sales` — in-store + online (Customer **B only**)
| column | type | notes |
|---|---|---|
| `sales_date` | DATE | |
| `product_id` | STRING | |
| `channel` | STRING | `in_store` \| `online` — "channel", "fulfillment type" |
| `units` | BIGINT | |
| `revenue` | DECIMAL(18,2) | |

> **Customer B only:** attach this instead of `promo_performance`. B's `sales_daily`
> and `market_share` already include the online channel.

## Joins (best-practices §1b lever 3)

- `sales_daily.product_id` → products (`many-to-one`) — for product_name/brand if a
  products dim is attached; brand/category are denormalized onto `sales_daily`.
- `sales_daily.store_id` → stores (`many-to-one`) — region/channel come denormalized.
- `promo_performance.product_id` → `sales_daily.product_id` (A/C).
- `omnichannel_sales.product_id` → `sales_daily.product_id` (B).

Since brand/category/region are denormalized onto `sales_daily`, most questions
need **no joins** — keep it that way for speed.

## Trusted assets (SQL functions — best-practices §1b lever 4)

Register the certified UC functions so Genie **invokes the exact formula** instead
of re-deriving it. These are the same functions the app's metric tools call, so
Genie and the tools agree to the decimal.

- `${catalog}.gold.f_market_share(p_category, p_region, p_period)` → certified market share.
- `${catalog}.gold.f_promo_lift(p_product_id, p_promo_id)` → certified promo lift **(A/C only)**.

## SQL expressions (§1b lever 4 — reusable measures)

- **measure** `total_revenue` = `SUM(revenue)`
- **measure** `total_units` = `SUM(units)`
- **measure** `avg_share_pct` = `AVG(share_pct)`
- **filter** `latest_period` = `period = (SELECT MAX(period) FROM ${catalog}.gold.market_share)`

## General instructions (§1b lever 6 — ONE block, last resort)

> Cerebro is a bakery-franchise market-analytics assistant. Currency is USD; round
> revenue to whole dollars and share/lift to one decimal. "Market share" = `share_pct`
> from `market_share`. When a certified function exists for the metric, prefer it.
> **[Customer C only]** Market share uses a trailing-4-week revenue-weighted method;
> say so when reporting share. **[Customer B only]** Sales are omnichannel (in-store +
> online); break out by `channel` when the user asks about online vs store. There is
> no promotions data for Customer B — if asked about promo lift, say it isn't tracked.

## ~10 sample questions (starter prompts)

1. What was total revenue last quarter by category?
2. Which brand has the highest market share in the Northeast?
3. Show market share for the Bread category by region for 2026-Q2.
4. What are the top 5 brands by revenue this year?
5. How did units sold trend month over month?
6. Which region grew revenue the most versus the prior period?
7. What's our market share trend for our top brand over the last four periods?
8. Which categories are we under-indexed in (low share)?  *(A/C/B)*
9. **[A/C]** Which promotion drove the biggest lift, and by how much?
10. **[B]** How does online compare to in-store revenue for our top category?

## Certified benchmark Q&A (ground-truth SQL — for Benchmarks, NOT instructions)

> Keep these in the **Benchmarks** tab (they're the answer key). Never paste them
> as example SQL (best-practices §1b lever 5 warning). Aim for ≥5; failure codes
> map to the lever that fixes them.

**Q1. "Which brand has the highest market share in the Northeast for 2026-Q2?"**
```sql
SELECT brand, share_pct
FROM ${catalog}.gold.market_share
WHERE region = 'Northeast' AND period = '2026-Q2'
ORDER BY share_pct DESC
LIMIT 1;
```

**Q2. "Total revenue by category for 2026-Q2."**
```sql
SELECT category, SUM(revenue) AS total_revenue
FROM ${catalog}.gold.sales_daily
WHERE sales_date BETWEEN DATE'2026-04-01' AND DATE'2026-06-30'
GROUP BY category
ORDER BY total_revenue DESC;
```

**Q3. "Certified market share for Bread in the Northeast, 2026-Q2."** (trusted asset)
```sql
SELECT * FROM ${catalog}.gold.f_market_share('Bread', 'Northeast', '2026-Q2');
```

**Q4. [A/C] "Which promotion had the highest lift?"**
```sql
SELECT promo_id, lift_pct
FROM ${catalog}.gold.promo_performance
ORDER BY lift_pct DESC
LIMIT 1;
```

**Q5. [B] "Online vs in-store revenue for the top category, 2026-Q2."**
```sql
SELECT channel, SUM(revenue) AS revenue
FROM ${catalog}.gold.omnichannel_sales
WHERE sales_date BETWEEN DATE'2026-04-01' AND DATE'2026-06-30'
GROUP BY channel
ORDER BY revenue DESC;
```

## Per-customer summary

| Customer | Catalog | Tables attached | Promo? | Notes |
|---|---|---|---|---|
| A | `cerebro_a` | sales_daily, market_share, promo_performance | ✅ | baseline methodology |
| B | `cerebro_b` | sales_daily, market_share, omnichannel_sales | ❌ | omnichannel; NO promo table/instructions |
| C | `cerebro_c` | sales_daily, market_share, promo_performance | ✅ | share = trailing-4-week revenue-weighted |
| dev | `cerebro_dev` | same as A | ✅ | reference build |

## Definition of Done

- [ ] Space created per catalog on a **serverless** warehouse, scoped to `${catalog}.gold`.
- [ ] Correct tables attached per customer (B: omnichannel, no promo; A/C: promo).
- [ ] `f_market_share` (all) and `f_promo_lift` (A/C) registered as trusted assets.
- [ ] Synonyms + entity-matching added for `region`, `category`, `brand`.
- [ ] General-instruction block set (with the C trailing-4-week + B omnichannel notes).
- [ ] ≥5 benchmark Q&A loaded and scored; the space id written to `${var.genie_space_id}`.
