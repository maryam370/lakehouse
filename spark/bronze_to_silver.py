from pyspark.sql import functions as F, Window
from common import get_spark

spark = get_spark("bronze_to_silver")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.ops")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lh.ops.rejects (
        table_name STRING,
        pk STRING,
        reason STRING,
        row_json STRING,
        rejected_at TIMESTAMP
    ) USING iceberg
""")

TABLES = {
    "customers": {
        "pk": "customer_id",
        "cols": """
            customer_id INT,
            full_name STRING,
            email STRING,
            city STRING,
            created_at LONG
        """,
        "select": [
            "customer_id",
            "full_name",
            "email",
            "city",
            "timestamp_micros(created_at) AS created_at"
        ],
        "rule": "email IS NOT NULL",
        "reason": "missing email"
    },
    "products": {
        "pk": "product_id",
        "cols": """
            product_id INT,
            name STRING,
            category STRING,
            price STRING
        """,
        "select": [
            "product_id",
            "name",
            "category",
            "CAST(price AS DECIMAL(10,2)) AS price"
        ],
        "rule": "price > 0",
        "reason": "price must be positive"
    },
    "orders": {
        "pk": "order_id",
        "cols": """
            order_id INT,
            customer_id INT,
            product_id INT,
            quantity INT,
            amount STRING,
            status STRING,
            created_at LONG,
            updated_at LONG
        """,
        "select": [
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "CAST(amount AS DECIMAL(10,2)) AS amount",
            "status",
            "timestamp_micros(created_at) AS created_at",
            "timestamp_micros(updated_at) AS updated_at"
        ],
        "rule": "amount > 0 AND quantity > 0",
        "reason": "amount and quantity must be positive"
    }
}

bronze = (
    spark.table("lh.bronze.cdc_raw")
    .where("value IS NOT NULL")
)

for name, t in TABLES.items():
    pk = t["pk"]

    envelope = (
        "op STRING, "
        "ts_ms LONG, "
        "before STRUCT<%s>, "
        "after STRUCT<%s>"
    ) % (t["cols"], t["cols"])

    ev = (
        bronze
        .where(F.col("topic") == f"shop.public.{name}")
        .select(
            "kafka_offset",
            F.from_json("value", envelope).alias("e")
        )
        .select(
            "kafka_offset",
            "e.op",
            "e.ts_ms",
            F.when(
                F.col("e.op") == "d",
                F.col("e.before")
            )
            .otherwise(F.col("e.after"))
            .alias("row")
        )
    )

    # Keep only the newest event for each primary key
    w = (
        Window
        .partitionBy(f"row.{pk}")
        .orderBy(
            F.col("ts_ms").desc(),
            F.col("kafka_offset").desc()
        )
    )

    latest = (
        ev
        .withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
    )

    deleted = (
        latest
        .where("op = 'd'")
        .select("row.*")
    )

    typed = (
        latest
        .where("op <> 'd'")
        .select("row.*")
        .selectExpr(*t["select"])
    )

    ok = f"coalesce(({t['rule']}), false)"

    good = typed.where(ok)
    bad = typed.where(f"NOT {ok}")

    target = f"lh.silver.{name}"

    if not spark.catalog.tableExists(target):
        (
            good.limit(0)
            .writeTo(target)
            .using("iceberg")
            .create()
        )

    good.createOrReplaceTempView("upd")

    (
        bad.select(pk)
        .unionByName(deleted.select(pk))
        .createOrReplaceTempView("gone")
    )

    spark.sql(
        f"""
        DELETE FROM {target}
        WHERE {pk} IN (
            SELECT {pk}
            FROM gone
        )
        """
    )

    spark.sql(
        f"""
        MERGE INTO {target} t
        USING upd u
        ON t.{pk} = u.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    spark.sql(
        f"""
        DELETE FROM lh.ops.rejects
        WHERE table_name = '{name}'
        """
    )

    (
        bad.select(
            F.lit(name).alias("table_name"),
            F.col(pk).cast("string").alias("pk"),
            F.lit(t["reason"]).alias("reason"),
            F.to_json(F.struct(*bad.columns)).alias("row_json"),
            F.current_timestamp().alias("rejected_at")
        )
        .writeTo("lh.ops.rejects")
        .append()
    )

    print(
        "RESULT",
        name,
        "good rows:",
        good.count(),
        "rejected rows:",
        bad.count()
    )
