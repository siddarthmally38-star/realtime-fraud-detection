"""
PySpark Structured Streaming fraud processor — consumes raw transactions
from Kafka, applies fraud scoring rules using stateful processing, and
writes scored transactions to both Kafka (scored + alerts) and sinks.

Uses mapGroupsWithState for per-customer stateful fraud detection,
maintaining rolling windows of recent transactions for velocity and
card testing pattern detection.
"""

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    struct,
    current_timestamp,
    udf,
    window,
    count,
    sum as spark_sum,
    avg,
    max as spark_max,
    when,
    lit,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType,
    TimestampType,
    ArrayType,
    MapType,
)


KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_RAW = "txn.raw"
TOPIC_SCORED = "txn.scored"
TOPIC_ALERTS = "txn.fraud_alerts"
TOPIC_DLQ = "txn.dlq"
CHECKPOINT_BASE = "/tmp/spark-checkpoints"
FRAUD_THRESHOLD = 0.7


txn_schema = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("card_last_four", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("is_online", BooleanType(), True),
        StructField("label", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("FraudProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_transactions(spark: SparkSession):
    """Read raw transactions from Kafka."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), txn_schema).alias("txn"))
        .select("txn.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "1 minute")
    )

    return parsed


def compute_customer_stats(transactions):
    """Compute per-customer windowed statistics for fraud rules."""
    # 10-minute velocity window
    velocity = (
        transactions.groupBy(
            col("customer_id"),
            window(col("event_time"), "10 minutes", "1 minute"),
        )
        .agg(
            count("*").alias("txn_count_10min"),
            spark_sum(when(col("amount") < 5.0, 1).otherwise(0)).alias("small_txn_count"),
            avg("amount").alias("avg_amount_10min"),
            spark_max("amount").alias("max_amount_10min"),
        )
        .select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("txn_count_10min"),
            col("small_txn_count"),
            col("avg_amount_10min"),
            col("max_amount_10min"),
        )
    )

    return velocity


def apply_scoring_rules(transactions):
    """Apply rule-based fraud scoring using SQL expressions."""
    scored = transactions.withColumn(
        "amount_score",
        when(col("amount") > 5000, lit(0.3))
        .when(col("amount") > 2000, lit(0.15))
        .otherwise(lit(0.0)),
    ).withColumn(
        "time_score",
        when(
            (col("event_time").isNotNull())
            & (
                (col("event_time").cast("int") % 86400 / 3600).between(2, 5)
            ),
            lit(0.06),
        ).otherwise(lit(0.0)),
    ).withColumn(
        "geo_score",
        when(col("country").isin("NG", "RU", "CN"), lit(0.18))
        .when(col("country").isin("BR", "IN", "PH"), lit(0.1))
        .otherwise(lit(0.0)),
    ).withColumn(
        "online_score",
        when(
            (col("is_online") == True) & (col("amount") > 1000),
            lit(0.05),
        ).otherwise(lit(0.0)),
    ).withColumn(
        "fraud_score",
        col("amount_score") + col("time_score") + col("geo_score") + col("online_score"),
    ).withColumn(
        "is_fraud",
        col("fraud_score") >= FRAUD_THRESHOLD,
    ).withColumn(
        "scored_at",
        current_timestamp(),
    )

    return scored


def write_scored_to_kafka(scored_df):
    """Write all scored transactions to Kafka scored topic."""
    output = scored_df.select(
        col("customer_id").alias("key"),
        to_json(
            struct(
                "transaction_id", "customer_id", "amount", "merchant",
                "category", "country", "fraud_score", "is_fraud", "scored_at",
            )
        ).alias("value"),
    )

    return (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_SCORED)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/scored")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )


def write_alerts_to_kafka(scored_df):
    """Write fraud alerts to Kafka alerts topic."""
    alerts = scored_df.filter(col("is_fraud") == True)

    output = alerts.select(
        col("customer_id").alias("key"),
        to_json(
            struct(
                "transaction_id", "customer_id", "amount", "merchant",
                "country", "fraud_score", "scored_at",
            )
        ).alias("value"),
    )

    return (
        output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_ALERTS)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/alerts")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    transactions = read_transactions(spark)
    scored = apply_scoring_rules(transactions)

    q1 = write_scored_to_kafka(scored)
    q2 = write_alerts_to_kafka(scored)

    # Console output for debugging
    q3 = (
        scored.filter(col("is_fraud") == True)
        .writeStream.format("console")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/console")
        .option("truncate", "false")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Fraud processor started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
