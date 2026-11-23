from common import get_spark

spark = get_spark("check")

for t in [
    "bronze.cdc_raw",
    "silver.customers",
    "silver.products",
    "silver.orders",
    "ops.rejects",
    "gold.daily_sales",
    "gold.sales_by_category",
    "gold.customer_value",
]:
    try:
        print("RESULT", t, spark.table("lh." + t).count())
    except Exception:
        print("RESULT", t, "not created yet")