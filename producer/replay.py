"""Replay PaySim rows into Kafka as JSON events, simulating a live payment stream.

    python replay.py --csv /data/raw/paysim.csv --rate 500
"""
import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

SIM_START = datetime(2026, 1, 1)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--rate", type=int, default=500, help="events per second")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,
        acks="all",  # wait for the broker to persist each batch (fire-and-forget lost 735 events once)
        retries=10,
        retry_backoff_ms=200,
        max_in_flight_requests_per_connection=1,  # retries can't reorder events
    )
    failed = 0

    def on_error(exc):
        nonlocal failed
        failed += 1

    sent, t0 = 0, time.time()
    step, rows_in_step = None, 0
    with open(args.csv, newline="") as f:
        for row in csv.DictReader(f):
            # PaySim only has hour-level `step`. Spread rows inside the hour *monotonically*
            # (50 ms apart, the busiest hour has ~51K rows < 72K slots) so event time never goes backwards;
            # an earlier `sent % 3600` version wrapped around and made ~440K events look late.
            if row["step"] != step:
                step, rows_in_step = row["step"], 0
            event_ts = SIM_START + timedelta(hours=int(step), milliseconds=50 * rows_in_step)
            rows_in_step += 1
            event = {
                "txn_id": sent + 1,  # PaySim has no id; row order is stable, so the line number works
                "event_ts": event_ts.isoformat(),
                "txn_type": row["type"],
                "amount": float(row["amount"]),
                "account_id": row["nameOrig"],
                "old_balance_orig": float(row["oldbalanceOrg"]),
                "new_balance_orig": float(row["newbalanceOrig"]),
                "dest_id": row["nameDest"],
                "old_balance_dest": float(row["oldbalanceDest"]),
                "new_balance_dest": float(row["newbalanceDest"]),
                "is_fraud": int(row["isFraud"]),  # label is carried only to score the rules, never used by them
            }
            # Key by account so one account's events stay ordered within a partition.
            producer.send(args.topic, key=row["nameOrig"], value=event).add_errback(on_error)
            sent += 1
            if args.limit and sent >= args.limit:
                break
            expected = sent / args.rate
            elapsed = time.time() - t0
            if expected > elapsed:
                time.sleep(expected - elapsed)

    producer.flush()
    elapsed = time.time() - t0
    print(f"sent {sent} events in {elapsed:.1f}s ({sent / elapsed:.0f}/s), delivery failures: {failed}")


if __name__ == "__main__":
    main()
