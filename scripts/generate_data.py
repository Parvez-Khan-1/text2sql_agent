import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)
OUT = "/home/claude/text2sql_poc/data/csv"
os.makedirs(OUT, exist_ok=True)

def rand_date(start="2023-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return (s + timedelta(seconds=random.randint(0, int((e-s).total_seconds())))).strftime("%Y-%m-%d %H:%M:%S")

def write_csv(name, headers, rows):
    path = f"{OUT}/{name}.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)
    print(f"  Written {len(rows)} rows → {name}.csv")

# ── 1. issuers ─────────────────────────────────────────────────────────────
issuers = []
issuer_names = ["Chase Bank","Bank of America","Wells Fargo","Citibank","US Bank",
                "Capital One","HSBC","Barclays","Deutsche Bank","BNP Paribas"]
for i, name in enumerate(issuer_names, 1):
    issuers.append({
        "issuer_id": f"ISS{i:03d}",
        "issuer_name": name,
        "country_code": random.choice(["US","UK","DE","FR","AU","CA"]),
        "bank_code": f"BNK{random.randint(1000,9999)}",
        "swift_code": f"SWIFT{i:04d}",
        "currency": random.choice(["USD","EUR","GBP","AUD","CAD"]),
        "is_active": random.choice([True, True, True, False]),
        "onboarded_date": rand_date("2018-01-01","2021-12-31"),
        "contact_email": f"ops@{name.lower().replace(' ','')}.com",
        "risk_tier": random.choice(["LOW","MEDIUM","HIGH"])
    })
write_csv("issuers", list(issuers[0].keys()), issuers)
issuer_ids = [r["issuer_id"] for r in issuers]

# ── 2. merchants ───────────────────────────────────────────────────────────
merchants = []
merchant_names = ["Amazon","Walmart","Target","Best Buy","Costco","Home Depot",
                  "Starbucks","McDonald's","Shell","Delta Airlines","Uber","Netflix",
                  "Apple Store","Nike","Zara","Marriott","Hilton","Lyft","DoorDash","Spotify"]
mcc_map = {"Amazon":"5969","Walmart":"5411","Target":"5411","Best Buy":"5734",
           "Costco":"5300","Home Depot":"5251","Starbucks":"5812","McDonald's":"5814",
           "Shell":"5541","Delta Airlines":"4511","Uber":"4121","Netflix":"7841",
           "Apple Store":"5734","Nike":"5661","Zara":"5621","Marriott":"7011",
           "Hilton":"7011","Lyft":"4121","DoorDash":"5812","Spotify":"7841"}
for i, name in enumerate(merchant_names, 1):
    merchants.append({
        "merchant_id": f"MER{i:04d}",
        "merchant_name": name,
        "mcc_code": mcc_map[name],
        "category": random.choice(["Retail","Food & Beverage","Travel","Entertainment","Transport","Utilities"]),
        "country_code": random.choice(["US","UK","DE","FR","AU"]),
        "city": random.choice(["New York","London","Berlin","Paris","Sydney","Chicago","LA"]),
        "is_high_risk": random.choice([False, False, False, True]),
        "onboarded_date": rand_date("2019-01-01","2022-12-31"),
        "monthly_volume_usd": round(random.uniform(10000, 5000000), 2),
        "chargeback_rate": round(random.uniform(0.001, 0.05), 4),
        "acquiring_bank": random.choice(["Chase","Citi","Wells Fargo","US Bank"]),
        "terminal_count": random.randint(1, 500)
    })
write_csv("merchants", list(merchants[0].keys()), merchants)
merchant_ids = [r["merchant_id"] for r in merchants]

# ── 3. cards ───────────────────────────────────────────────────────────────
cards = []
for i in range(1, 201):
    issuer = random.choice(issuer_ids)
    cards.append({
        "card_id": f"CRD{i:05d}",
        "masked_pan": f"4{random.randint(100,999)} **** **** {random.randint(1000,9999)}",
        "card_type": random.choice(["CREDIT","DEBIT","PREPAID"]),
        "card_brand": random.choice(["VISA","MASTERCARD","AMEX","DISCOVER"]),
        "issuer_id": issuer,
        "cardholder_name": f"Cardholder {i}",
        "expiry_date": f"{random.randint(1,12):02d}/{random.randint(25,29)}",
        "is_active": random.choice([True, True, True, False]),
        "credit_limit_usd": round(random.uniform(500, 50000), 2) if random.random() > 0.3 else None,
        "issued_date": rand_date("2020-01-01","2023-06-30"),
        "last_used_date": rand_date("2023-01-01","2024-12-31"),
        "country_of_issue": random.choice(["US","UK","DE","FR","AU","CA"]),
        "is_corporate": random.choice([False, False, True])
    })
write_csv("cards", list(cards[0].keys()), cards)
card_ids = [r["card_id"] for r in cards]

# ── 4. authorizations ──────────────────────────────────────────────────────
auth_statuses = ["APPROVED","DECLINED","PENDING","TIMEOUT","REFERRED"]
auth_weights  = [0.70, 0.15, 0.05, 0.05, 0.05]
decline_reasons = ["INSUFFICIENT_FUNDS","CARD_EXPIRED","DO_NOT_HONOR","INVALID_CVV",
                   "VELOCITY_LIMIT","FRAUD_SUSPECTED",None]
authorizations = []
for i in range(1, 501):
    status = random.choices(auth_statuses, weights=auth_weights)[0]
    authorizations.append({
        "auth_id": f"AUTH{i:06d}",
        "card_id": random.choice(card_ids),
        "merchant_id": random.choice(merchant_ids),
        "auth_amount_usd": round(random.uniform(1, 5000), 2),
        "currency": random.choice(["USD","EUR","GBP"]),
        "auth_status": status,
        "decline_reason": random.choice(decline_reasons) if status == "DECLINED" else None,
        "auth_timestamp": rand_date(),
        "response_code": random.choice(["00","05","14","51","54","57","61","62"]),
        "auth_type": random.choice(["ONLINE","CONTACTLESS","CHIP","SWIPE","MANUAL"]),
        "is_international": random.choice([False, False, True]),
        "mcc_code": random.choice(list(mcc_map.values())),
        "network": random.choice(["VISA","MASTERCARD","AMEX"]),
        "risk_score": round(random.uniform(0, 100), 1),
        "is_3ds_verified": random.choice([True, True, False])
    })
write_csv("authorizations", list(authorizations[0].keys()), authorizations)
auth_ids_approved = [r["auth_id"] for r in authorizations if r["auth_status"] == "APPROVED"]

# ── 5. transactions ────────────────────────────────────────────────────────
transactions = []
for i in range(1, 401):
    auth = random.choice(auth_ids_approved)
    auth_rec = next(r for r in authorizations if r["auth_id"] == auth)
    transactions.append({
        "txn_id": f"TXN{i:07d}",
        "auth_id": auth,
        "card_id": auth_rec["card_id"],
        "merchant_id": auth_rec["merchant_id"],
        "txn_amount_usd": round(float(auth_rec["auth_amount_usd"]) * random.uniform(0.95, 1.0), 2),
        "txn_currency": auth_rec["currency"],
        "txn_timestamp": rand_date(),
        "txn_type": random.choice(["PURCHASE","REFUND","CASH_ADVANCE","BALANCE_INQUIRY"]),
        "txn_status": random.choice(["SETTLED","PENDING","FAILED","REVERSED"]),
        "settlement_date": rand_date("2024-01-01","2024-12-31"),
        "pos_entry_mode": random.choice(["CHIP","SWIPE","CONTACTLESS","ECOM","MANUAL"]),
        "acquirer_ref": f"ACQ{random.randint(100000,999999)}",
        "interchange_fee_usd": round(random.uniform(0.1, 50), 2),
        "processing_fee_usd": round(random.uniform(0.05, 5), 2),
        "is_cross_border": random.choice([False, False, True]),
        "exchange_rate": round(random.uniform(0.8, 1.3), 4) if random.random() > 0.7 else 1.0
    })
write_csv("transactions", list(transactions[0].keys()), transactions)
txn_ids = [r["txn_id"] for r in transactions]

# ── 6. clearing ────────────────────────────────────────────────────────────
clearing = []
for i in range(1, 351):
    txn = random.choice(txn_ids)
    txn_rec = next(r for r in transactions if r["txn_id"] == txn)
    clearing.append({
        "clearing_id": f"CLR{i:06d}",
        "txn_id": txn,
        "merchant_id": txn_rec["merchant_id"],
        "clearing_amount_usd": txn_rec["txn_amount_usd"],
        "clearing_currency": txn_rec["txn_currency"],
        "clearing_date": rand_date("2024-01-01","2024-12-31"),
        "clearing_status": random.choice(["CLEARED","PENDING","FAILED","RECONCILED"]),
        "batch_id": f"BATCH{random.randint(1000,9999)}",
        "settlement_bank": random.choice(["Chase","Citi","Wells Fargo"]),
        "net_settlement_usd": round(float(txn_rec["txn_amount_usd"]) * 0.98, 2),
        "clearing_cycle": random.choice(["T+1","T+2","T+3"]),
        "reconciliation_status": random.choice(["MATCHED","UNMATCHED","PENDING"]),
        "file_reference": f"FILE{random.randint(10000,99999)}",
        "interchange_amount_usd": round(random.uniform(0.1, 50), 2),
        "scheme_fee_usd": round(random.uniform(0.01, 2), 4)
    })
write_csv("clearing", list(clearing[0].keys()), clearing)

# ── 7. chargebacks ────────────────────────────────────────────────────────
cb_reasons = ["FRAUD","ITEM_NOT_RECEIVED","NOT_AS_DESCRIBED","DUPLICATE_CHARGE",
              "SUBSCRIPTION_CANCELLED","CREDIT_NOT_PROCESSED","UNRECOGNIZED_CHARGE"]
chargebacks = []
for i in range(1, 101):
    txn = random.choice(txn_ids)
    txn_rec = next(r for r in transactions if r["txn_id"] == txn)
    chargebacks.append({
        "chargeback_id": f"CB{i:06d}",
        "txn_id": txn,
        "card_id": txn_rec["card_id"],
        "merchant_id": txn_rec["merchant_id"],
        "cb_amount_usd": txn_rec["txn_amount_usd"],
        "cb_currency": txn_rec["txn_currency"],
        "cb_reason_code": random.choice(cb_reasons),
        "cb_status": random.choice(["OPEN","WON","LOST","WITHDRAWN","UNDER_REVIEW"]),
        "filed_date": rand_date("2024-01-01","2024-10-31"),
        "resolution_date": rand_date("2024-03-01","2024-12-31"),
        "issuer_id": random.choice(issuer_ids),
        "is_friendly_fraud": random.choice([False, False, True]),
        "evidence_submitted": random.choice([True, False]),
        "days_to_resolve": random.randint(10, 120),
        "representment_count": random.randint(0, 3),
        "final_liability": random.choice(["MERCHANT","ISSUER","ACQUIRER","SCHEME"])
    })
write_csv("chargebacks", list(chargebacks[0].keys()), chargebacks)

# ── 8. dispute_cases ──────────────────────────────────────────────────────
dispute_cases = []
for i in range(1, 81):
    txn = random.choice(txn_ids)
    txn_rec = next(r for r in transactions if r["txn_id"] == txn)
    dispute_cases.append({
        "dispute_id": f"DSP{i:05d}",
        "txn_id": txn,
        "card_id": txn_rec["card_id"],
        "merchant_id": txn_rec["merchant_id"],
        "dispute_type": random.choice(["FRAUD","BILLING_ERROR","SERVICE_DISPUTE","UNAUTHORIZED"]),
        "dispute_status": random.choice(["OPEN","RESOLVED","ESCALATED","WITHDRAWN","PENDING_INFO"]),
        "dispute_amount_usd": txn_rec["txn_amount_usd"],
        "opened_date": rand_date("2024-01-01","2024-10-31"),
        "closed_date": rand_date("2024-04-01","2024-12-31"),
        "assigned_agent": f"AGENT{random.randint(1,20):03d}",
        "priority": random.choice(["LOW","MEDIUM","HIGH","CRITICAL"]),
        "customer_contact_count": random.randint(1, 15),
        "resolution_type": random.choice(["REFUND","REJECTED","PARTIAL_REFUND","ESCALATED","WITHDRAWN"]),
        "sla_breached": random.choice([False, False, True]),
        "notes": random.choice(["Customer claims fraud","Duplicate charge found","Item never arrived",
                                "Subscription cancelled prior","Merchant unresponsive",None])
    })
write_csv("dispute_cases", list(dispute_cases[0].keys()), dispute_cases)

print("\n[done] All 8 CSVs generated successfully!")