"""Unit tests for the batch job's pure transformations (no Postgres, no Kafka).

    pip install -r requirements-dev.txt && pytest -q
"""
from datetime import date

import pytest
from pyspark.sql import SparkSession

from jobs.batch_daily import build_daily_summary, quality_checks


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("ledgerflow-test").getOrCreate()
    yield s
    s.stop()


@pytest.fixture
def day_df(spark):
    rows = [
        (date(2026, 1, 1), "TRANSFER", "C1", 100.00, 0),
        (date(2026, 1, 1), "TRANSFER", "C2", 250.50, 1),
        (date(2026, 1, 1), "PAYMENT", "C3", 10.25, 0),
    ]
    return spark.createDataFrame(rows, "txn_date date, txn_type string, account_id string, amount double, isFraud int")


def test_daily_summary_aggregates_per_type(day_df):
    out = {r["txn_type"]: r for r in build_daily_summary(day_df).collect()}
    assert out["TRANSFER"]["txn_count"] == 2
    assert float(out["TRANSFER"]["total_amount"]) == pytest.approx(350.50)
    assert out["TRANSFER"]["fraud_count"] == 1
    assert out["TRANSFER"]["fraud_rate"] == pytest.approx(0.5)
    assert out["PAYMENT"]["fraud_rate"] == 0


def test_quality_checks_pass_on_clean_data(day_df):
    assert all(v == 0 for v in quality_checks(day_df).values())


def test_quality_checks_catch_negative_amount(spark):
    bad = spark.createDataFrame(
        [(date(2026, 1, 1), "PAYMENT", "C9", -5.0, 0)],
        "txn_date date, txn_type string, account_id string, amount double, isFraud int",
    )
    assert quality_checks(bad)["negative_amount"] == 1


def test_load_day_maps_step_hours_to_calendar_day(spark, tmp_path, monkeypatch):
    import jobs.batch_daily as bd

    csv = tmp_path / "paysim.csv"
    csv.write_text(
        "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n"
        "1,PAYMENT,10.0,C1,0,0,M1,0,0,0,0\n"     # 2026-01-01 01:00
        "23,TRANSFER,20.0,C2,0,0,C9,0,0,1,0\n"   # 2026-01-01 23:00
        "25,CASH_OUT,30.0,C3,0,0,C8,0,0,0,0\n"   # 2026-01-02 01:00
    )
    monkeypatch.setattr(bd, "RAW_CSV", str(csv))
    assert bd.load_day(spark, date(2026, 1, 1)).count() == 2
    assert bd.load_day(spark, date(2026, 1, 2)).count() == 1
