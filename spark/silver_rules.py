from pyspark.sql import functions as F, Window

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


def expected_columns(t):
    """Column names the job knows about for one table."""
    return [c.strip().split()[0] for c in t["cols"].split(",") if c.strip()]


def rule_expr(t):
    """SQL expression that is true for rows passing the quality rule (null counts as failed)."""
    return f"coalesce(({t['rule']}), false)"


def latest_events(events, name, t):
    """Newest CDC event per primary key.

    events: DataFrame with columns topic, kafka_offset, value (Debezium JSON).
    Returns columns: kafka_offset, op, ts_ms, row (struct), rn (always 1).
    For deletes, row holds the 'before' image so the key is still available.
    """
    pk = t["pk"]
    envelope = (
        "op STRING, "
        "ts_ms LONG, "
        "before STRUCT<%s>, "
        "after STRUCT<%s>"
    ) % (t["cols"], t["cols"])

    ev = (
        events
        .where(F.col("topic") == f"shop.public.{name}")
        .select(
            "kafka_offset",
            F.from_json("value", envelope).alias("e")
        )
        .select(
            "kafka_offset",
            "e.op",
            "e.ts_ms",
            F.when(F.col("e.op") == "d", F.col("e.before"))
            .otherwise(F.col("e.after"))
            .alias("row")
        )
    )

    w = (
        Window
        .partitionBy(f"row.{pk}")
        .orderBy(F.col("ts_ms").desc(), F.col("kafka_offset").desc())
    )

    return (
        ev
        .withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
    )
