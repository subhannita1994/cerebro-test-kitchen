# Databricks notebook source
# =============================================================================
# apply_ddl.py — schema-as-code runner (ALL customers). Backs the `apply_ddl` job.
# -----------------------------------------------------------------------------
# Applies the numbered, idempotent DDL artifacts in ddl/ against ${var.catalog}:
#   1. substitute the literal ${catalog} token with the target catalog name,
#   2. execute each statement via spark.sql(), in filename order.
#
# When ${var.pipeline_variant} == "omnichannel" (Customer B) it ALSO applies the
# divergence overlay ddl/variants/customer_b/010_omnichannel.sql AFTER the base
# files (drops promo objects, adds online_orders + omnichannel_sales).
#
# No GitHub Actions — this runner IS the apply mechanism, run per catalog by the
# DAB job. Everything it runs is idempotent, so re-running is safe.
# =============================================================================

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession


def get_params():
    try:
        dbutils  # type: ignore  # noqa: F821
        dbutils.widgets.text("catalog", "cerebro_dev", "Target catalog")  # type: ignore # noqa: F821
        dbutils.widgets.dropdown("pipeline_variant", "baseline",  # type: ignore # noqa: F821
                                 ["baseline", "omnichannel", "altlogic"])
        dbutils.widgets.text("ddl_root", "", "DDL dir override (optional)")  # type: ignore # noqa: F821
        g = dbutils.widgets.get  # type: ignore  # noqa: F821
        return g("catalog"), g("pipeline_variant"), g("ddl_root")
    except NameError:
        import argparse
        p = argparse.ArgumentParser()
        p.add_argument("--catalog", default="cerebro_dev")
        p.add_argument("--pipeline_variant", default="baseline",
                       choices=["baseline", "omnichannel", "altlogic"])
        p.add_argument("--ddl_root", default="")
        a = p.parse_args()
        return a.catalog, a.pipeline_variant, a.ddl_root


def find_repo_root() -> str:
    """Walk up from this file (or cwd, in a notebook) to the dir with databricks.yml."""
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
    # Fallback: assume this file lives at <root>/src/pipelines/apply_ddl.py
    try:
        return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except NameError:
        return os.getcwd()


def split_statements(sql_text: str, catalog: str) -> list[str]:
    """Substitute ${catalog}, drop comment-only lines, split on ';' — but NOT on
    semicolons that appear INSIDE single-quoted string literals (our COMMENT
    clauses contain ';'). SQL escapes an inner quote as ''."""
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
    print(f"[apply_ddl] applying {os.path.basename(path)}")
    with open(path, "r") as f:
        text = f.read()
    for stmt in split_statements(text, catalog):
        spark.sql(stmt)


def main():
    catalog, variant, ddl_root_override = get_params()
    spark = SparkSession.builder.getOrCreate()

    ddl_root = ddl_root_override or os.path.join(find_repo_root(), "ddl")
    print(f"[apply_ddl] catalog={catalog} variant={variant} ddl_root={ddl_root}")

    # Base numbered files (000..003), in filename order.
    base_files = sorted(glob.glob(os.path.join(ddl_root, "[0-9][0-9][0-9]_*.sql")))
    if not base_files:
        raise FileNotFoundError(f"No numbered DDL files found under {ddl_root}")
    for path in base_files:
        apply_file(spark, path, catalog)

    # Customer B divergence overlay — ONLY for omnichannel.
    if variant == "omnichannel":
        overlay = os.path.join(ddl_root, "variants", "customer_b", "010_omnichannel.sql")
        if not os.path.exists(overlay):
            raise FileNotFoundError(f"Omnichannel overlay missing: {overlay}")
        print("[apply_ddl] variant=omnichannel -> applying Customer B overlay")
        apply_file(spark, overlay, catalog)
    else:
        print("[apply_ddl] baseline schema (no overlay)")

    print("[apply_ddl] done.")


if __name__ == "__main__":
    main()
