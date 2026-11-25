import sys

from pyspark import StorageLevel
from pyspark.sql import functions as F
from common import get_spark
from silver_rules import TABLES, expected_columns, latest_events, rule_expr

CONSUMER = "bronze_to_silver"
FULL_REFRESH = "--full-refresh" in sys.argv

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

# Where we stopped reading Bronze, per Kafka topic partition.
spark.sql("""
    CREATE TABLE IF NOT EXISTS lh.ops.bronze_offsets (
        consumer STRING,
        topic STRING,
        kafka_partition INT,
        last_offset BIGINT,
        updated_at TIMESTAMP
    ) USING iceberg
""")

# Columns seen in the source that Silver does not know about yet.
spark.sql("""
    CREATE TABLE IF NOT EXISTS lh.ops.schema_drift (
        table_name STRING,
        column_name STRING,
        first_seen TIMESTAMP
    ) USING iceberg
""")

if FULL_REFRESH:
    print("FULL REFRESH requested: clearing watermark")
    spark.sql(f"DELETE FROM lh.ops.bronze_offsets WHERE consumer = '{CONSUMER}'")

# ---- decide which offsets to process in this run ---------------------------
bronze = spark.table("lh.bronze.cdc_raw")

high = (
    bronze.groupBy("topic", "kafka_partition")
    .agg(F.max("kafka_offset").alias("high"))
)

low = (
    spark.table("lh.ops.bronze_offsets")
    .where(F.col("consumer") == CONSUMER)
    .select("topic", "kafka_partition", F.col("last_offset").alias("low"))
)

range_rows = (
    high.join(low, ["topic", "kafka_partition"], "left")
    .withColumn("low", F.coalesce(F.col("low"), F.lit(-1).cast("long")))
    .where("high > low")
    .collect()
)

if not range_rows:
    print("RESULT no new bronze events, nothing to do")
    spark.stop()
    sys.exit(0)

ranges = spark.createDataFrame(
    [(r["topic"], int(r["kafka_partition"]), int(r["low"]), int(r["high"])) for r in range_rows],
    "topic string, kafka_partition int, low long, high long"
)

# Fixed upper bound: events that arrive while this job runs wait for the next run.
new_events = (
    bronze
    .join(F.broadcast(ranges), ["topic", "kafka_partition"])
    .where((F.col("kafka_offset") > F.col("low")) & (F.col("kafka_offset") <= F.col("high")))
    .where("value IS NOT NULL")
)

print("RESULT processing offsets:",
      [(r["topic"], int(r["kafka_partition"]), int(r["low"]), int(r["high"])) for r in range_rows])

for name, t in TABLES.items():
    pk = t["pk"]
    target = f"lh.silver.{name}"
    topic_events = new_events.where(F.col("topic") == f"shop.public.{name}")

    # Schema drift: fields in the source payload that we do not map yet.
    seen = (
        topic_events
        .select(F.map_keys(
            F.from_json("value", "struct<after:map<string,string>>").getField("after")
        ).alias("ks"))
        .select(F.explode("ks").alias("c"))
        .distinct()
        .collect()
    )
    unknown = sorted({r["c"] for r in seen} - set(expected_columns(t)))
    if unknown:
        print("WARNING schema drift in", name, "- unmapped columns:", unknown)
        drift = (
            spark.createDataFrame(
                [(name, c) for c in unknown],
                "table_name string, column_name string"
            )
            .withColumn("first_seen", F.current_timestamp())
        )
        known = spark.table("lh.ops.schema_drift").select("table_name", "column_name")
        (
            drift.join(known, ["table_name", "column_name"], "left_anti")
            .writeTo("lh.ops.schema_drift")
            .append()
        )

    # Newest event per key in this batch. Persisted so every step below sees the same rows.
    latest = latest_events(new_events, name, t).persist(StorageLevel.MEMORY_AND_DISK)

    deleted = latest.where("op = 'd'").select("row.*")

    typed = (
        latest
        .where("op <> 'd'")
        .select("row.*")
        .selectExpr(*t["select"])
    )

    ok = rule_expr(t)
    good = typed.where(ok)
    bad = typed.where(f"NOT {ok}")

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

    spark.sql(f"DELETE FROM {target} WHERE {pk} IN (SELECT {pk} FROM gone)")

    spark.sql(f"""
        MERGE INTO {target} t
        USING upd u
        ON t.{pk} = u.{pk}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    latest.select(F.col(f"row.{pk}").cast("string").alias("pk")).createOrReplaceTempView("touched")

    spark.sql(f"""
        DELETE FROM lh.ops.rejects
        WHERE table_name = '{name}'
          AND pk IN (SELECT pk FROM touched)
    """)

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

    print("RESULT", name, "good rows:", good.count(), "rejected rows:", bad.count(),
          "deleted keys:", deleted.count())

    latest.unpersist()

(
    spark.createDataFrame(
        [(CONSUMER, r["topic"], int(r["kafka_partition"]), int(r["high"])) for r in range_rows],
        "consumer string, topic string, kafka_partition int, last_offset long"
    )
    .withColumn("updated_at", F.current_timestamp())
    .createOrReplaceTempView("wm_new")
)

spark.sql("""
    MERGE INTO lh.ops.bronze_offsets t
    USING wm_new s
    ON t.consumer = s.consumer
       AND t.topic = s.topic
       AND t.kafka_partition = s.kafka_partition
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("RESULT watermark advanced")
