import os
from pyspark.sql import SparkSession


def get_spark(app_name):
    c = "spark.sql.catalog.lh"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(c, "org.apache.iceberg.spark.SparkCatalog")
        .config(c + ".catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(
            c + ".uri",
            "jdbc:postgresql://platform-db.lakehouse.svc.cluster.local:5432/iceberg_catalog"
        )
        .config(c + ".jdbc.user", os.environ["PG_USER"])
        .config(c + ".jdbc.password", os.environ["PG_PASSWORD"])
        .config(c + ".jdbc.schema-version", "V1")
        .config(c + ".warehouse", "s3://warehouse")
        .config(c + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(
            c + ".s3.endpoint",
            "http://seaweedfs.lakehouse.svc.cluster.local:8333"
        )
        .config(c + ".s3.path-style-access", "true")
        .config(c + ".s3.region", "us-east-1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark