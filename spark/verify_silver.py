import sys

from pyspark.sql import functions as F
from common import get_spark
from silver_rules import TABLES, latest_events, rule_expr

spark = get_spark("verify_silver")

bronze = spark.table("lh.bronze.cdc_raw").where("value IS NOT NULL")
rejects = spark.table("lh.ops.rejects")

failures = 0

for name, t in TABLES.items():
    pk = t["pk"]
    latest = latest_events(bronze, name, t)

    typed = (
        latest
        .where("op <> 'd'")
        .select("row.*")
        .selectExpr(*t["select"])
    )

    ok = rule_expr(t)
    good = typed.where(ok)
    bad = typed.where(f"NOT {ok}")

    silver = spark.table(f"lh.silver.{name}")
    cols = silver.columns

    missing = good.select(*cols).exceptAll(silver.select(*cols)).count()
    extra = silver.select(*cols).exceptAll(good.select(*cols)).count()

    expected_bad = bad.select(F.col(pk).cast("string").alias("pk"))
    actual_bad = rejects.where(F.col("table_name") == name).select("pk")
    rej_missing = expected_bad.exceptAll(actual_bad).count()
    rej_extra = actual_bad.exceptAll(expected_bad).count()

    passed = (missing + extra + rej_missing + rej_extra) == 0
    print(
        "RESULT", "PASS" if passed else "FAIL", name,
        "silver_missing:", missing, "silver_extra:", extra,
        "rejects_missing:", rej_missing, "rejects_extra:", rej_extra
    )
    if not passed:
        failures += 1

if failures:
    print("RESULT verify FAILED for", failures, "table(s)")
    sys.exit(1)

print("RESULT verify PASSED: Silver equals a full recompute")
