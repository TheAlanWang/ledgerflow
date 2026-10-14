"""Stream rules tested on static DataFrames (watermark is a no-op outside streaming)."""
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from jobs.stream_alerts import rule_alerts, window_metrics

COLS = ("txn_id long, event_ts timestamp, txn_type string, amount double, account_id string, "
        "old_balance_orig double, new_balance_orig double, dest_id string, "
        "old_balance_dest double, new_balance_dest double, is_fraud int")


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("ledgerflow-stream-test").getOrCreate()
    yield s
    s.stop()


T0 = datetime(2026, 1, 1, 10, 0, 0)


def test_exact_drain_and_ghost_transfer(spark):
    rows = [
        # 1: TRANSFER emptying the account exactly -> EXACT_DRAIN (dest also stays 0 -> GHOST_TRANSFER)
        (1, T0, "TRANSFER", 500.0, "C1", 500.0, 0.0, "C9", 0.0, 0.0, 1),
        # 2: CASH_OUT emptying the account exactly -> EXACT_DRAIN only
        (2, T0, "CASH_OUT", 80.0, "C2", 80.0, 0.0, "C8", 10.0, 90.0, 1),
        # 3: normal TRANSFER, partial amount, dest credited -> no alert
        (3, T0, "TRANSFER", 50.0, "C3", 500.0, 450.0, "C7", 100.0, 150.0, 0),
        # 4: PAYMENT that empties the account -> rules only cover TRANSFER/CASH_OUT
        (4, T0, "PAYMENT", 20.0, "C4", 20.0, 0.0, "M1", 0.0, 0.0, 0),
    ]
    out = sorted((r["txn_id"], r["rule"]) for r in rule_alerts(spark.createDataFrame(rows, COLS)).collect())
    assert out == [(1, "EXACT_DRAIN"), (1, "GHOST_TRANSFER"), (2, "EXACT_DRAIN")]


def test_window_metrics_group_by_5min_and_type(spark):
    rows = [
        (1, T0, "PAYMENT", 10.0, "C1", 0.0, 0.0, "M1", 0.0, 0.0, 0),
        (2, T0 + timedelta(minutes=2), "PAYMENT", 15.0, "C2", 0.0, 0.0, "M1", 0.0, 0.0, 0),
        (3, T0 + timedelta(minutes=6), "PAYMENT", 5.0, "C3", 0.0, 0.0, "M1", 0.0, 0.0, 0),
    ]
    out = sorted((r["window_start"], r["txn_count"], float(r["total_amount"]))
                 for r in window_metrics(spark.createDataFrame(rows, COLS)).collect())
    assert out == [(T0, 2, 25.0), (T0 + timedelta(minutes=5), 1, 5.0)]
