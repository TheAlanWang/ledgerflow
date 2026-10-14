"""Streaming job: Kafka `transactions` -> rule-based fraud alerts + windowed metrics in Postgres.

Rules (stateless, evaluated per event). They were chosen from a data analysis of PaySim, not guesses;
see README "Rule design":
  - EXACT_DRAIN    : TRANSFER/CASH_OUT whose amount equals the origin account's entire balance (to the cent)
  - GHOST_TRANSFER : TRANSFER where the destination balance is 0 before and after, i.e. the money never
                     arrives on the receiving ledger (a reconciliation break)

Windowed metrics (stateful): per 5-minute event-time window and txn_type: count and total amount,
with a 10-minute watermark bounding late data.
"""
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("txn_id", LongType()),
    StructField("event_ts", StringType()),
    StructField("txn_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("account_id", StringType()),
    StructField("old_balance_orig", DoubleType()),
    StructField("new_balance_orig", DoubleType()),
    StructField("dest_id", StringType()),
    StructField("old_balance_dest", DoubleType()),
    StructField("new_balance_dest", DoubleType()),
    StructField("is_fraud", IntegerType()),
])

DB = {
    "url": os.environ.get("LEDGER_DB_URL", "jdbc:postgresql://localhost:55432/ledger"),
    "user": os.environ.get("LEDGER_DB_USER", "ledger"),
    "password": os.environ.get("LEDGER_DB_PASSWORD", "ledger"),
    "driver": "org.postgresql.Driver",
}
CHECKPOINT = os.environ.get("LEDGER_DATA_DIR", "/opt/ledgerflow/data") + "/checkpoints"

ALERT_COLS = ["txn_id", "event_ts", "rule", "txn_type", "account_id", "dest_id", "amount", "is_fraud"]


def read_events(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
        .option("subscribe", "transactions")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)  # bound micro-batch size for steady latency
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )


def exact_drain() -> Column:
    return (
        F.col("txn_type").isin("TRANSFER", "CASH_OUT")
        & (F.col("old_balance_orig") > 0)
        & (F.abs(F.col("amount") - F.col("old_balance_orig")) < 0.01)
    )


def ghost_transfer() -> Column:
    return (
        (F.col("txn_type") == "TRANSFER")
        & (F.col("amount") > 0)
        & (F.col("old_balance_dest") == 0)
        & (F.col("new_balance_dest") == 0)
    )


def rule_alerts(events: DataFrame) -> DataFrame:
    """One row per (transaction, rule) hit."""
    drain = events.filter(exact_drain()).withColumn("rule", F.lit("EXACT_DRAIN"))
    ghost = events.filter(ghost_transfer()).withColumn("rule", F.lit("GHOST_TRANSFER"))
    return drain.select(*ALERT_COLS).unionByName(ghost.select(*ALERT_COLS))


def window_metrics(events: DataFrame) -> DataFrame:
    """Per 5-minute event-time window and txn_type: count + total amount (late data bounded to 10 min)."""
    return (
        events.withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"), "txn_type")
        .agg(F.count(F.lit(1)).alias("txn_count"), F.round(F.sum("amount"), 2).alias("total_amount"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "txn_type",
            "txn_count",
            "total_amount",
        )
    )


# Upsert SQL per sink. foreachBatch is at-least-once: if the job stops after writing a batch but
# before committing its checkpoint, that batch is replayed on restart. Writing through a staging
# table + ON CONFLICT makes the replay a no-op, so results are effectively exactly-once.
UPSERT = {
    "alerts": (
        "INSERT INTO alerts (txn_id, event_ts, rule, txn_type, account_id, dest_id, amount, is_fraud) "
        "SELECT txn_id, event_ts, rule, txn_type, account_id, dest_id, amount, is_fraud FROM {staging} "
        "ON CONFLICT (txn_id, rule) DO NOTHING"
    ),
    "stream_metrics": (
        "INSERT INTO stream_metrics (window_start, window_end, txn_type, txn_count, total_amount) "
        "SELECT window_start, window_end, txn_type, txn_count, total_amount FROM {staging} "
        "ON CONFLICT (window_start, txn_type) DO UPDATE SET "
        "txn_count = EXCLUDED.txn_count, total_amount = EXCLUDED.total_amount, window_end = EXCLUDED.window_end"
    ),
}


def run_sql(spark: SparkSession, sql: str) -> None:
    """Execute one statement over JDBC from the driver (the Spark image has no psycopg2)."""
    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(DB["url"], DB["user"], DB["password"])
    try:
        stmt = conn.createStatement()
        stmt.executeUpdate(sql)
        stmt.close()
    finally:
        conn.close()


def write_table(df: DataFrame, table: str, name: str):
    staging = f"{table}_staging"

    def to_postgres(batch_df: DataFrame, _batch_id: int) -> None:
        # 1) overwrite a per-sink staging table with this micro-batch, 2) upsert into the target.
        batch_df.write.format("jdbc").options(dbtable=staging, **DB).mode("overwrite").save()
        run_sql(batch_df.sparkSession, UPSERT[table].format(staging=staging))

    return (
        df.writeStream.queryName(name).foreachBatch(to_postgres)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT}/{name}")
        .start()
    )


def main() -> None:
    spark = SparkSession.builder.appName("ledgerflow-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    events = read_events(spark)
    queries = [
        write_table(rule_alerts(events), "alerts", "rule_alerts"),
        write_table(window_metrics(events), "stream_metrics", "window_metrics"),
    ]
    # Log per-query throughput every 30s (micro-batch progress) so we can measure events/s.
    while not spark.streams.awaitAnyTermination(timeout=30):
        for q in queries:
            p = q.lastProgress
            if p and p["numInputRows"]:
                print(
                    f"[progress] {q.name} batch={p['batchId']} rows={p['numInputRows']} "
                    f"in/s={p['inputRowsPerSecond']:.0f} processed/s={p['processedRowsPerSecond']:.0f} "
                    f"batchMs={p['durationMs'].get('triggerExecution')}",
                    flush=True,
                )


if __name__ == "__main__":
    main()
