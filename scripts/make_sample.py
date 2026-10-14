"""Generate a small synthetic CSV in PaySim's schema (for smoke tests before downloading the real dataset).

    python scripts/make_sample.py --rows 20000 --out data/raw/paysim.csv
"""
import argparse
import csv
import random

HEADER = "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud".split(",")
TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]

ap = argparse.ArgumentParser()
ap.add_argument("--rows", type=int, default=20000)
ap.add_argument("--out", default="data/raw/paysim.csv")
args = ap.parse_args()

random.seed(7)
with open(args.out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(HEADER)
    for _ in range(args.rows):
        w.writerow([random.randint(1, 72), random.choice(TYPES), round(random.expovariate(1 / 5000), 2),
                    f"C{random.randint(1, 3000)}", 0, 0, f"M{random.randint(1, 500)}", 0, 0,
                    1 if random.random() < 0.01 else 0, 0])
print(f"wrote {args.rows} rows to {args.out}")
