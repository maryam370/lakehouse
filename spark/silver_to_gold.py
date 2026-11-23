from common import get_spark

spark = get_spark("silver_to_gold")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")

spark.sql("""
    CREATE OR REPLACE TABLE lh.gold.daily_sales
    USING iceberg AS
    SELECT
        CAST(created_at AS DATE) AS order_date,
        COUNT(*) AS orders,
        SUM(quantity) AS units,
        SUM(amount) AS revenue
    FROM lh.silver.orders
    WHERE status <> 'CANCELLED'
    GROUP BY CAST(created_at AS DATE)
""")

spark.sql("""
    CREATE OR REPLACE TABLE lh.gold.sales_by_category
    USING iceberg AS
    SELECT
        p.category,
        COUNT(*) AS orders,
        SUM(o.quantity) AS units,
        SUM(o.amount) AS revenue
    FROM lh.silver.orders o
    JOIN lh.silver.products p
        ON o.product_id = p.product_id
    WHERE o.status <> 'CANCELLED'
    GROUP BY p.category
""")

spark.sql("""
    CREATE OR REPLACE TABLE lh.gold.customer_value
    USING iceberg AS
    SELECT
        c.customer_id,
        c.full_name,
        c.city,
        COUNT(*) AS orders,
        SUM(o.amount) AS total_spent,
        MAX(o.created_at) AS last_order_at
    FROM lh.silver.orders o
    JOIN lh.silver.customers c
        ON o.customer_id = c.customer_id
    WHERE o.status <> 'CANCELLED'
    GROUP BY
        c.customer_id,
        c.full_name,
        c.city
""")

print("RESULT gold tables rebuilt")