from common import get_spark

spark = get_spark("streaming_bronze")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lh.bronze.cdc_raw (
        topic STRING,
        kafka_partition INT,
        kafka_offset BIGINT,
        kafka_ts TIMESTAMP,
        key STRING,
        value STRING
    ) USING iceberg
""")

src = (
    spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "kafka.ingestion.svc.cluster.local:9092"
        )
        .option(
            "subscribePattern",
            r"shop\.public\..*"
        )
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 2000)
        .load()
)

out = (
    src.selectExpr(
        "topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "timestamp AS kafka_ts",
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value"
    )
)

query = (
    out.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/checkpoints/bronze_cdc")
        .toTable("lh.bronze.cdc_raw")
)

query.awaitTermination()