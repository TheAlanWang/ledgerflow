"""Daily batch job: raw PaySim CSV -> cleaned Parquet partition -> data-quality checks -> daily summary.

Run for one logical day (idempotent: re-running the same day overwrites that day's output only):

    python jobs/batch_daily.py --date 2026-01-03
"""
import argparse
import os
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("LEDGER_DATA_DIR", "data")
RAW_CSV = f"{DATA_DIR}/raw/paysim.csv"
CLEAN_DIR = f"{DATA_DIR}/clean/transactions"

# PaySim's `step` is hours since the simulation started; anchor it to a fixed calendar date.
SIM_START = datetime(2026, 1, 1)

# Explicit schema: inferSchema would cost an extra full pass over the 470 MB CSV on every run.
RAW_SCHEMA = (
    "step INT, type STRING, amount DOUBLE, nameOrig STRING, oldbalanceOrg DOUBLE, newbalanceOrig DOUBLE, "
    "nameDest STRING, oldbalanceDest DOUBLE, newbalanceDest DOUBLE, isFraud INT, isFlaggedFraud INT"
)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ledgerflow-batch")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def load_day(spark: SparkSession, day: date) -> DataFrame:
    """Read the raw CSV and keep only rows whose event time falls on `day`."""
    raw = spark.read.option("header", True).schema(RAW_SCHEMA).csv(RAW_CSV)
    with_ts = (
        raw.withColumn("event_ts", F.expr(f"timestamp'{SIM_START:%Y-%m-%d %H:%M:%S}' + make_interval(0,0,0,0,step,0,0)"))
        .withColumn("txn_date", F.to_date("event_ts"))
        .withColumnRenamed("type", "txn_type")
        .withColumnRenamed("nameOrig", "account_id")
    )
    return with_ts.filter(F.col("txn_date") == F.lit(day.isoformat()))


def quality_checks(df: DataFrame) -> dict[str, int]:
    """Return {check_name: failed_row_count}. A check passes when its count is 0."""
    return {
        "null_amount": df.filter(F.col("amount").isNull()).count(),
        "negative_amount": df.filter(F.col("amount") < 0).count(),
        "null_account": df.filter(F.col("account_id").isNull()).count(),
        "duplicate_rows": df.count() - df.dropDuplicates().count(),
    }


def build_daily_summary(df: DataFrame) -> DataFrame:
    """Aggregate one day of transactions into one row per (txn_date, txn_type).

    Output columns must match the `daily_summary` table in sql/init.sql:
        txn_date, txn_type, txn_count, total_amount, fraud_count, fraud_rate
    """
    is_fraud = F.when(F.col("isFraud") == 1, 1).otherwise(0)  # robust even if the flag isn't strictly 0/1
    return (
        df.groupBy("txn_date", "txn_type")
        .agg(
            F.count(F.lit(1)).cast("long").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.sum(is_fraud).cast("long").alias("fraud_count"),
        )
        .withColumn("fraud_rate", F.col("fraud_count") / F.col("txn_count"))
    )


def write_postgres(df: DataFrame, table: str, day: date) -> None:
    """Idempotent load: delete the day's rows, then append the new ones."""
    import psycopg2

    url = os.environ.get("LEDGER_DB_URL", "jdbc:postgresql://localhost:55432/ledger")
    user = os.environ.get("LEDGER_DB_USER", "ledger")
    pwd = os.environ.get("LEDGER_DB_PASSWORD", "ledger")
    host_db = url.split("//", 1)[1]
    host_port, dbname = host_db.split("/", 1)
    host, port = host_port.split(":")
    with psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE txn_date = %s", (day,))
    (
        df.write.format("jdbc")
        .option("url", url).option("dbtable", table)
        .option("user", user).option("password", pwd)
        .option("driver", "org.postgresql.Driver")
        .mode("append").save()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="logical day, YYYY-MM-DD")
    day = datetime.strptime(parser.parse_args().date, "%Y-%m-%d").date()

    spark = spark_session()
    df = load_day(spark, day).cache()

    results = quality_checks(df)
    dq = spark.createDataFrame(
        [(day, name, failed, failed == 0) for name, failed in results.items()],
        "txn_date date, check_name string, failed_rows long, passed boolean",
    )
    write_postgres(dq, "dq_results", day)
    if any(results.values()):
        raise SystemExit(f"data-quality checks failed for {day}: {results}")

    df.write.mode("overwrite").partitionBy("txn_date").parquet(CLEAN_DIR)
    write_postgres(build_daily_summary(df), "daily_summary", day)
    print(f"{day}: {df.count()} rows processed")


if __name__ == "__main__":
    main()
