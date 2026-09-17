# Databricks notebook source
# =============================================================================
# register_functions.py — UC function registrar (ALL customers). Backs the
# `register_functions` job.
# -----------------------------------------------------------------------------
# Creates the UC SQL functions the AI swimlane's tools call:
#   * ${catalog}.gold.f_market_share  — ALWAYS (all customers/variants).
#   * ${catalog}.gold.f_promo_lift    — ONLY when ${var.enable_promo}=true
#                                        (Customers A/C + dev; NOT Customer B).
#
# Reads the .sql files in src/functions/, substitutes the literal ${catalog}
# token, and executes them. CREATE OR REPLACE => idempotent + re-runnable.
# =============================================================================

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_params():
    try:
        dbutils  # type: ignore  # noqa: F821
        dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")  # type: ignore # noqa: F821
        dbutils.widgets.dropdown("enable_promo", "true", ["true", "false"])  # type: ignore # noqa: F821
        dbutils.widgets.text("functions_root", "", "functions dir override")  # type: ignore # noqa: F821
        g = dbutils.widgets.get  # type: ignore  # noqa: F821
        return g("catalog"), g("enable_promo").lower() == "true", g("functions_root")
    except NameError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--catalog", default="cerebro_dev")
        p.add_argument("--enable_promo", default="true")
        p.add_argument("--functions_root", default="")
        a = p.parse_args()
        return a.catalog, a.enable_promo.lower() == "true", a.functions_root


def find_repo_root() -> str:
    starts = []
    try:
        starts.append(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
    starts.append(os.getcwd())
    for start in starts:
        d = start
        for _ in range(8):
            if os.path.exists(os.path.join(d, "databricks.yml")):
                return d
            parent = os.path.dirname(d)
            if parent == d:
                break
            d = parent
    try:
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except NameError:
        return os.getcwd()


def split_statements(sql_text: str, catalog: str) -> list[str]:
    """Substitute ${catalog}, drop comment-only lines, split on ';' — but NOT on
    semicolons INSIDE single-quoted string literals (our COMMENT clauses contain
    ';'). SQL escapes an inner quote as ''."""
    sql_text = sql_text.replace("${catalog}", catalog)
    kept = [ln for ln in sql_text.splitlines() if not ln.lstrip().startswith("--")]
    body = "\n".join(kept)

    stmts: list[str] = []
    buf: list[str] = []
    in_str = False
    i, n = 0, len(body)
    while i < n:
        ch = body[i]
        if ch == "'":
            if in_str and i + 1 < n and body[i + 1] == "'":  # escaped '' inside string
                buf.append("''")
                i += 2
                continue
            in_str = not in_str
            buf.append(ch)
        elif ch == ";" and not in_str:
            s = "".join(buf).strip()
            if s:
                stmts.append(s)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def apply_file(spark: SparkSession, path: str, catalog: str) -> None:
    print(f"[register_functions] applying {os.path.basename(path)}")
    with open(path, "r") as f:
        text = f.read()
    for stmt in split_statements(text, catalog):
        spark.sql(stmt)


def main():
    catalog, enable_promo, root_override = get_params()
    spark = SparkSession.builder.getOrCreate()

    fns_root = root_override or os.path.join(find_repo_root(), "src", "functions")
    print(f"[register_functions] catalog={catalog} enable_promo={enable_promo} "
          f"root={fns_root}")

    # f_market_share — always.
    apply_file(spark, os.path.join(fns_root, "f_market_share.sql"), catalog)

    # f_promo_lift — only when promotions are enabled.
    if enable_promo:
        apply_file(spark, os.path.join(fns_root, "f_promo_lift.sql"), catalog)
        print("[register_functions] f_promo_lift registered (enable_promo=true)")
    else:
        print("[register_functions] SKIPPED f_promo_lift (enable_promo=false, e.g. Customer B)")

    print("[register_functions] done.")


if __name__ == "__main__":
    main()
