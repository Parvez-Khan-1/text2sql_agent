import sqlite3
import csv
import os

DB_PATH = "/home/claude/text2sql_poc/data/payments.db"
CSV_DIR = "/home/claude/text2sql_poc/data/csv"

# Column type hints — everything else defaults to TEXT
TYPE_MAP = {
    "auth_amount_usd": "REAL", "txn_amount_usd": "REAL", "clearing_amount_usd": "REAL",
    "cb_amount_usd": "REAL", "dispute_amount_usd": "REAL", "credit_limit_usd": "REAL",
    "monthly_volume_usd": "REAL", "interchange_fee_usd": "REAL", "processing_fee_usd": "REAL",
    "net_settlement_usd": "REAL", "interchange_amount_usd": "REAL", "scheme_fee_usd": "REAL",
    "exchange_rate": "REAL", "chargeback_rate": "REAL", "risk_score": "REAL",
    "terminal_count": "INTEGER", "days_to_resolve": "INTEGER", "representment_count": "INTEGER",
    "customer_contact_count": "INTEGER",
    "is_active": "INTEGER", "is_high_risk": "INTEGER", "is_international": "INTEGER",
    "is_3ds_verified": "INTEGER", "is_cross_border": "INTEGER", "is_friendly_fraud": "INTEGER",
    "evidence_submitted": "INTEGER", "sla_breached": "INTEGER", "is_corporate": "INTEGER",
}

def infer_type(col):
    return TYPE_MAP.get(col, "TEXT")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

for csv_file in sorted(os.listdir(CSV_DIR)):
    if not csv_file.endswith(".csv"):
        continue
    table = csv_file.replace(".csv", "")
    path = os.path.join(CSV_DIR, csv_file)

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        rows = list(reader)

    col_defs = ", ".join(f'"{c}" {infer_type(c)}' for c in cols)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

    for row in rows:
        values = []
        for c in cols:
            v = row[c]
            if v in ("None", "", "NULL"):
                values.append(None)
            elif infer_type(c) == "INTEGER":
                values.append(1 if v in ("True","1","true") else 0 if v in ("False","0","false") else (int(v) if v else None))
            elif infer_type(c) == "REAL":
                values.append(float(v) if v else None)
            else:
                values.append(v)
        placeholders = ",".join(["?"] * len(cols))
        cur.execute(f'INSERT INTO "{table}" VALUES ({placeholders})', values)

    conn.commit()
    print(f"  [ok] {table}: {len(rows)} rows loaded")

# Add indexes for common join columns
indexes = [
    ("transactions", "card_id"), ("transactions", "merchant_id"), ("transactions", "auth_id"),
    ("authorizations", "card_id"), ("authorizations", "merchant_id"),
    ("chargebacks", "txn_id"), ("chargebacks", "merchant_id"),
    ("clearing", "txn_id"), ("dispute_cases", "txn_id"),
]
for tbl, col in indexes:
    cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{tbl}_{col}" ON "{tbl}" ("{col}")')

conn.commit()
conn.close()
print(f"\n[done] SQLite DB created at {DB_PATH}")