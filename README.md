# LedgerFlow

Batch + streaming pipeline for payment transactions, running locally on Docker.

```
PaySim CSV ─► producer ─► Kafka ─► Spark Structured Streaming ─► Postgres.alerts      (stream)
      └──────────────────► Airflow @daily ─► Spark batch job ─► Parquet + Postgres.daily_summary   (batch)
```

- **Batch** (`jobs/batch_daily.py`, scheduled by `dags/ledgerflow_daily.py`): reads one logical day, runs data-quality checks, writes a day-partitioned Parquet table, and loads a daily summary into Postgres. Every run is **idempotent**: re-running a day replaces only that day.
- **Stream** (`jobs/stream_alerts.py`): consumes Kafka events, raises rule-based fraud alerts (EXACT_DRAIN, GHOST_TRANSFER), and maintains 5-minute windowed metrics with a 10-minute watermark and checkpointed recovery.

## Data
Download the PaySim dataset from Kaggle ("Synthetic Financial Datasets For Fraud Detection", ~6.3M rows) and save it as `data/raw/paysim.csv`.
For a quick smoke test without Kaggle: `python scripts/make_sample.py` (20K synthetic rows, same schema).
`scripts/demo/*.csv` are tiny replays from the first rule version (kept for reference). Score the current rules with `psql -U ledger -f sql/score_rules.sql`.

## Run
```bash
docker compose up -d --build postgres kafka airflow   # Airflow UI: http://localhost:8080 (password in `docker compose logs airflow`)
docker compose exec airflow airflow dags backfill ledgerflow_daily -s 2026-01-01 -e 2026-01-31
docker compose --profile stream up -d spark-stream
docker compose --profile replay run --rm producer
docker compose exec postgres psql -U ledger -c "select * from daily_summary order by txn_date limit 10;"
```

Postgres is published on **localhost:55432**.

## Results (full PaySim, 6,362,620 transactions, MacBook, local Docker)
| Layer | Result |
|---|---|
| Batch backfill | 31 days via Airflow, 62/62 tasks succeeded; row totals match the raw file exactly; 124/124 data-quality checks passed; ~14 s per day (~570K rows) |
| Ingestion | 6,362,620 events replayed in 358 s (~17.8K events/s, producer-bound), **0 delivery failures** (`acks=all` + retries) |
| Stream rules | median **235K rows/s** processed per micro-batch (peak 329K); windowed metrics ~1.2 s per batch (p90 1.2 s) |
| Rule quality | **precision 98.7%, recall 99.4%** (8,272 alerts, 8,166 true fraud of 8,213); identical to the offline analysis |

## Rule design (data-driven)
The first version used intuition-based rules, and both failed on real data:
| Rule | Alerts | Precision | Recall | Why it failed |
|---|---|---|---|---|
| single TRANSFER/CASH_OUT > 200K | 1,197,669 | 0.5% | 66.6% | large transfers are normal in PaySim |
| >5 txns per account in 5 min | 0 | — | 0% | origin accounts appear at most 3 times in the whole dataset |
| account fully drained (amount ≥ balance) | 1,188,086 | 0.7% | 97.7% | legit cash-outs also empty accounts |
| inbound bursts to one destination | — | — | — | fraud destinations are almost never reused (8,169 distinct of 8,213) |

Analysis with PySpark found two reconciliation-style signals, which are the shipped rules:
| Rule | Alerts | Precision | Recall |
|---|---|---|---|
| EXACT_DRAIN: TRANSFER/CASH_OUT amount equals the full origin balance (to the cent) | 8,018 | 100% | 97.6% |
| GHOST_TRANSFER: TRANSFER where destination balance is 0 before and after | 4,174 | 97.5% | 49.5% |
| either | 8,272 | **98.7%** | **99.4%** |

**Caveat:** these signals are partly artifacts of how the PaySim simulator models fraudsters (they always take the full balance) and of its destination bookkeeping. Real-world fraud would not be this clean; the point of the project is the pipeline and the data-driven iteration, not the rule itself.

## Bugs found during the full-data run
- **735 events lost** with fire-and-forget producing (`acks=1`, no retries) when the laptop slept mid-run → switched to `acks=all`, retries, `max_in_flight=1`, and a delivery-failure counter → 0 lost.
- **~440K events dropped as late data** because synthetic in-hour timestamps used `sent % 3600` and wrapped backwards → made timestamps monotonic within each hour (50 ms apart) → all but the final open window counted.
- **A backfilled day was silently skipped**: Airflow backfill skips dates already marked successful (left over from a smoke test) even after the downstream tables were truncated → re-ran with `--reset-dagruns`.

## Notes
- Velocity windows emit only after the watermark (10 min) passes the window end **and** a later micro-batch arrives. Events older than the watermark are dropped as late data, by design.

## Tests
```bash
python -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt && pytest -q
```

## Milestones
- [x] Stage 1: batch job: daily summary, data-quality gates, idempotent re-runs (verified on 20K-row sample)
- [x] Stage 2: Airflow DAG: FileSensor → Spark batch (`airflow dags test` succeeded)
- [x] Stage 3: streaming alerts: high-value + 5-min velocity windows with watermark (verified with `scripts/demo`)
- [x] Run on the real 6.3M-row PaySim dataset and backfill 31 days
- [x] Measure throughput, rule precision/recall (see Results)
