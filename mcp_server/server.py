"""
Card Payments MCP Server — HTTP/SSE transport
Tools  : list_tables, get_table_schema, get_table_metadata,
         get_sample_data, get_table_relationships, validate_sql, execute_sql
Resources: domain_glossary, business_rules, er_diagram_text
"""

import sqlite3
import json
import re
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from mcp.server.fastmcp import FastMCP
from mcp import types

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payments-mcp")

DB_PATH = "./data/payments.db"

# ─────────────────────────────────────────────────────────────────────────────
# Rich metadata catalogue
# ─────────────────────────────────────────────────────────────────────────────
TABLE_METADATA = {
    "issuers": {
        "description": "Card-issuing banks and financial institutions that issue payment cards to cardholders.",
        "business_domain": "Card Issuing",
        "primary_key": "issuer_id",
        "columns": {
            "issuer_id":       "Unique identifier for the issuer (e.g. ISS001)",
            "issuer_name":     "Full legal name of the bank/FI",
            "country_code":    "ISO-2 country code where the issuer is domiciled",
            "bank_code":       "Internal bank routing code",
            "swift_code":      "SWIFT/BIC code for international wire identification",
            "currency":        "Primary settlement currency (ISO-4217)",
            "is_active":       "1=active issuer, 0=deactivated",
            "onboarded_date":  "Date the issuer joined the network",
            "contact_email":   "Operational contact for reconciliation/dispute queries",
            "risk_tier":       "Risk classification: LOW / MEDIUM / HIGH",
        }
    },
    "merchants": {
        "description": "Businesses and vendors that accept card payments. Each merchant has an MCC code defining their business category.",
        "business_domain": "Merchant Management",
        "primary_key": "merchant_id",
        "columns": {
            "merchant_id":         "Unique merchant identifier (e.g. MER0001)",
            "merchant_name":       "Trading name of the merchant",
            "mcc_code":            "Merchant Category Code — ISO-18245 4-digit code classifying business type",
            "category":            "Human-readable category: Retail, Food & Beverage, Travel, etc.",
            "country_code":        "ISO-2 country where merchant is registered",
            "city":                "City of primary merchant location",
            "is_high_risk":        "1=merchant flagged as high-risk, 0=standard",
            "onboarded_date":      "Date merchant was onboarded to the network",
            "monthly_volume_usd":  "Average monthly transaction volume in USD",
            "chargeback_rate":     "Rolling chargeback rate (ratio). >0.01 is high per scheme rules",
            "acquiring_bank":      "Name of the merchant's acquiring bank",
            "terminal_count":      "Number of active POS terminals",
        }
    },
    "cards": {
        "description": "Payment cards issued to cardholders, including credit, debit and prepaid cards.",
        "business_domain": "Card Lifecycle",
        "primary_key": "card_id",
        "columns": {
            "card_id":            "Unique card identifier (e.g. CRD00001)",
            "masked_pan":         "Masked Primary Account Number — only first 4 and last 4 digits visible",
            "card_type":          "CREDIT / DEBIT / PREPAID",
            "card_brand":         "Card network brand: VISA, MASTERCARD, AMEX, DISCOVER",
            "issuer_id":          "FK → issuers.issuer_id",
            "cardholder_name":    "Name printed on the card",
            "expiry_date":        "Card expiry in MM/YY format",
            "is_active":          "1=card active, 0=blocked or expired",
            "credit_limit_usd":   "Credit limit in USD (NULL for debit/prepaid cards)",
            "issued_date":        "Date the card was issued",
            "last_used_date":     "Date of the last transaction on this card",
            "country_of_issue":   "ISO-2 country where the card was issued",
            "is_corporate":       "1=corporate card, 0=consumer card",
        }
    },
    "authorizations": {
        "description": "Real-time authorization requests sent by merchants to the network for approval before a transaction is completed.",
        "business_domain": "Authorization & Fraud",
        "primary_key": "auth_id",
        "columns": {
            "auth_id":           "Unique authorization identifier (e.g. AUTH000001)",
            "card_id":           "FK → cards.card_id",
            "merchant_id":       "FK → merchants.merchant_id",
            "auth_amount_usd":   "Amount requested for authorization in USD",
            "currency":          "Transaction currency (ISO-4217)",
            "auth_status":       "APPROVED / DECLINED / PENDING / TIMEOUT / REFERRED",
            "decline_reason":    "Reason for decline: INSUFFICIENT_FUNDS, CARD_EXPIRED, FRAUD_SUSPECTED, etc.",
            "auth_timestamp":    "Timestamp of the authorization request",
            "response_code":     "ISO-8583 response code (00=approved, 05=do not honor, 51=insuff funds…)",
            "auth_type":         "Channel: ONLINE / CONTACTLESS / CHIP / SWIPE / MANUAL",
            "is_international":  "1=cross-border authorization",
            "mcc_code":          "MCC code of the merchant at time of auth",
            "network":           "Card network processing the auth: VISA / MASTERCARD / AMEX",
            "risk_score":        "Real-time fraud risk score 0-100 (higher = riskier)",
            "is_3ds_verified":   "1=3D Secure authentication passed",
        }
    },
    "transactions": {
        "description": "Completed financial transactions — the settled record after a successful authorization.",
        "business_domain": "Transaction Processing",
        "primary_key": "txn_id",
        "columns": {
            "txn_id":              "Unique transaction identifier (e.g. TXN0000001)",
            "auth_id":             "FK → authorizations.auth_id",
            "card_id":             "FK → cards.card_id",
            "merchant_id":         "FK → merchants.merchant_id",
            "txn_amount_usd":      "Final settled transaction amount in USD",
            "txn_currency":        "Transaction currency",
            "txn_timestamp":       "When the transaction occurred",
            "txn_type":            "PURCHASE / REFUND / CASH_ADVANCE / BALANCE_INQUIRY",
            "txn_status":          "SETTLED / PENDING / FAILED / REVERSED",
            "settlement_date":     "Date the funds were settled",
            "pos_entry_mode":      "How the card was read: CHIP / SWIPE / CONTACTLESS / ECOM / MANUAL",
            "acquirer_ref":        "Acquirer reference number for reconciliation",
            "interchange_fee_usd": "Fee paid to the issuer in USD",
            "processing_fee_usd":  "Network processing fee in USD",
            "is_cross_border":     "1=transaction crossed country borders",
            "exchange_rate":       "FX rate applied (1.0 if same currency)",
        }
    },
    "clearing": {
        "description": "The clearing process where transactions are batched, reconciled and submitted for settlement between acquirer and issuer.",
        "business_domain": "Clearing & Settlement",
        "primary_key": "clearing_id",
        "columns": {
            "clearing_id":             "Unique clearing record identifier (e.g. CLR000001)",
            "txn_id":                  "FK → transactions.txn_id",
            "merchant_id":             "FK → merchants.merchant_id",
            "clearing_amount_usd":     "Amount submitted for clearing",
            "clearing_currency":       "Currency of the clearing record",
            "clearing_date":           "Date clearing was submitted",
            "clearing_status":         "CLEARED / PENDING / FAILED / RECONCILED",
            "batch_id":                "Batch file identifier grouping multiple clearing records",
            "settlement_bank":         "Bank responsible for settling the funds",
            "net_settlement_usd":      "Amount after fees deducted (typically clearing_amount × 0.98)",
            "clearing_cycle":          "Settlement timing: T+1 / T+2 / T+3",
            "reconciliation_status":   "MATCHED / UNMATCHED / PENDING",
            "file_reference":          "Source file reference for audit trail",
            "interchange_amount_usd":  "Interchange fee component of clearing",
            "scheme_fee_usd":          "Card scheme (Visa/MC) fee component",
        }
    },
    "chargebacks": {
        "description": "Chargebacks initiated when a cardholder disputes a transaction with their issuer. The funds are reversed from the merchant pending resolution.",
        "business_domain": "Dispute & Chargeback",
        "primary_key": "chargeback_id",
        "columns": {
            "chargeback_id":       "Unique chargeback identifier (e.g. CB000001)",
            "txn_id":              "FK → transactions.txn_id",
            "card_id":             "FK → cards.card_id",
            "merchant_id":         "FK → merchants.merchant_id",
            "cb_amount_usd":       "Amount being charged back in USD",
            "cb_currency":         "Currency of the chargeback",
            "cb_reason_code":      "Reason: FRAUD / ITEM_NOT_RECEIVED / NOT_AS_DESCRIBED / DUPLICATE_CHARGE etc.",
            "cb_status":           "OPEN / WON / LOST / WITHDRAWN / UNDER_REVIEW",
            "filed_date":          "Date the chargeback was filed by the issuer",
            "resolution_date":     "Date the chargeback was resolved",
            "issuer_id":           "FK → issuers.issuer_id",
            "is_friendly_fraud":   "1=suspected friendly fraud (cardholder filed despite receiving goods)",
            "evidence_submitted":  "1=merchant submitted rebuttal evidence",
            "days_to_resolve":     "Number of days from filing to resolution",
            "representment_count": "How many times the merchant challenged the chargeback",
            "final_liability":     "Who bears the loss: MERCHANT / ISSUER / ACQUIRER / SCHEME",
        }
    },
    "dispute_cases": {
        "description": "Customer service dispute cases — broader than chargebacks, includes billing errors, fraud claims and service disputes managed by agents.",
        "business_domain": "Customer Disputes",
        "primary_key": "dispute_id",
        "columns": {
            "dispute_id":             "Unique dispute case identifier (e.g. DSP00001)",
            "txn_id":                 "FK → transactions.txn_id",
            "card_id":                "FK → cards.card_id",
            "merchant_id":            "FK → merchants.merchant_id",
            "dispute_type":           "FRAUD / BILLING_ERROR / SERVICE_DISPUTE / UNAUTHORIZED",
            "dispute_status":         "OPEN / RESOLVED / ESCALATED / WITHDRAWN / PENDING_INFO",
            "dispute_amount_usd":     "Amount under dispute in USD",
            "opened_date":            "Date dispute was opened",
            "closed_date":            "Date dispute was closed (NULL if still open)",
            "assigned_agent":         "Agent ID handling the dispute",
            "priority":               "Case priority: LOW / MEDIUM / HIGH / CRITICAL",
            "customer_contact_count": "Number of times customer contacted support",
            "resolution_type":        "REFUND / REJECTED / PARTIAL_REFUND / ESCALATED / WITHDRAWN",
            "sla_breached":           "1=dispute resolution SLA was breached",
            "notes":                  "Free text notes from the agent",
        }
    },
}

TABLE_RELATIONSHIPS = """
ENTITY RELATIONSHIPS (Card Payments Domain)
============================================

issuers (1) ──────────────────── (N) cards
    issuers.issuer_id = cards.issuer_id

cards (1) ──────────────────────────────────── (N) authorizations
    cards.card_id = authorizations.card_id

merchants (1) ──────────────────────────────── (N) authorizations
    merchants.merchant_id = authorizations.merchant_id

authorizations (1) ─────────────────────────── (N) transactions
    authorizations.auth_id = transactions.auth_id

cards (1) ──────────────────────────────────── (N) transactions
    cards.card_id = transactions.card_id

merchants (1) ──────────────────────────────── (N) transactions
    merchants.merchant_id = transactions.merchant_id

transactions (1) ───────────────────────────── (N) clearing
    transactions.txn_id = clearing.txn_id

transactions (1) ───────────────────────────── (N) chargebacks
    transactions.txn_id = chargebacks.txn_id

transactions (1) ───────────────────────────── (N) dispute_cases
    transactions.txn_id = dispute_cases.txn_id

issuers (1) ────────────────────────────────── (N) chargebacks
    issuers.issuer_id = chargebacks.issuer_id

merchants (1) ──────────────────────────────── (N) chargebacks
    merchants.merchant_id = chargebacks.merchant_id

merchants (1) ──────────────────────────────── (N) clearing
    merchants.merchant_id = clearing.merchant_id

TYPICAL JOIN PATHS
──────────────────
• Card → Auth → Transaction → Clearing   (full payment lifecycle)
• Transaction → Chargeback               (dispute initiated)
• Transaction → Dispute Case             (customer service case)
• Merchant + Chargeback                  (merchant chargeback analysis)
• Issuer → Card → Transaction            (issuer portfolio view)
"""

DOMAIN_GLOSSARY = """
CARD PAYMENTS DOMAIN GLOSSARY
==============================
Authorization  : Real-time approval check before funds are committed.
Settlement     : Final transfer of funds between acquirer and issuer.
Clearing       : Batch process that reconciles & submits txns for settlement.
Chargeback     : Forced reversal initiated by cardholder via their issuer.
Dispute        : Broader customer complaint — may or may not become a chargeback.
MCC            : Merchant Category Code — 4-digit ISO code for business type.
PAN            : Primary Account Number — the 16-digit card number.
3DS            : 3D Secure — additional cardholder authentication layer.
Interchange    : Fee paid by acquirer to issuer per transaction (~1-2% of amount).
Acquirer       : Bank that processes payments on behalf of the merchant.
Issuer         : Bank that issued the card to the cardholder.
Friendly Fraud : Cardholder files chargeback despite receiving goods/services.
Representment  : Merchant formally challenges a chargeback with evidence.
SLA            : Service Level Agreement — time limit for resolving disputes.
Response Code  : ISO-8583 code indicating auth result (00=OK, 05=denied, 51=funds).
T+1/T+2/T+3   : Settlement timing — days after transaction until funds move.
"""

BUSINESS_RULES = """
BUSINESS RULES & THRESHOLDS
============================
1. HIGH CHARGEBACK RATE   : merchants.chargeback_rate > 0.01 (1%) is scheme violation territory.
2. HIGH RISK SCORE        : authorizations.risk_score > 75 triggers manual review.
3. FRAUD THRESHOLD        : decline_reason = 'FRAUD_SUSPECTED' must be reported to issuers within 24h.
4. CLEARING WINDOW        : Merchants must submit clearing within T+3 or authorization expires.
5. CHARGEBACK WINDOW      : Cardholders have 120 days from txn_date to file a chargeback.
6. DISPUTE SLA            : All disputes must be resolved within 45 days (sla_breached = 1 if exceeded).
7. INTERCHANGE RATE       : Standard interchange = 1.5-2% of txn_amount. Cash advance = 3-5%.
8. INTERNATIONAL SURCHARGE: is_cross_border = 1 adds ~1.5% FX markup.
9. 3DS LIABILITY SHIFT    : If is_3ds_verified = 1 and fraud occurs, liability shifts to issuer.
10. MERCHANT RISK TIERS   : is_high_risk = 1 requires enhanced monitoring & monthly audits.
"""

# ─────────────────────────────────────────────────────────────────────────────
# FastMCP app
# ─────────────────────────────────────────────────────────────────────────────
mcp = FastMCP(
    "CardPaymentsMCP",
    instructions="MCP server for a Card Payments SQLite database with 8 tables covering "
                 "the full payment lifecycle: issuers, merchants, cards, authorizations, "
                 "transactions, clearing, chargebacks and dispute_cases.",
    host="0.0.0.0",
    port=8001,
)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ─── TOOLS ───────────────────────────────────────────────────────────────────

@mcp.tool()
def list_tables() -> str:
    """List all available tables in the payments database with a brief description."""
    result = []
    for table, meta in TABLE_METADATA.items():
        conn = get_db()
        row = conn.execute(f'SELECT COUNT(*) AS cnt FROM "{table}"').fetchone()
        conn.close()
        result.append({
            "table": table,
            "description": meta["description"],
            "business_domain": meta["business_domain"],
            "primary_key": meta["primary_key"],
            "row_count": row["cnt"],
        })
    return json.dumps(result, indent=2)


@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """
    Get the full schema (columns, types, constraints) for a specific table.
    Also returns business-friendly column descriptions.
    """
    if table_name not in TABLE_METADATA:
        return json.dumps({"error": f"Table '{table_name}' not found. Call list_tables() first."})
    conn = get_db()
    pragma = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    conn.close()
    meta = TABLE_METADATA[table_name]
    columns = []
    for row in pragma:
        col = row["name"]
        columns.append({
            "column": col,
            "type": row["type"],
            "nullable": not row["notnull"],
            "default": row["dflt_value"],
            "description": meta["columns"].get(col, ""),
        })
    return json.dumps({
        "table": table_name,
        "description": meta["description"],
        "business_domain": meta["business_domain"],
        "primary_key": meta["primary_key"],
        "columns": columns,
    }, indent=2)


@mcp.tool()
def get_table_metadata(table_name: str) -> str:
    """
    Get rich business metadata for a table including column meanings,
    domain context and relationships to other tables.
    """
    if table_name not in TABLE_METADATA:
        return json.dumps({"error": f"Unknown table: {table_name}"})
    meta = TABLE_METADATA[table_name]
    rel_lines = [l for l in TABLE_RELATIONSHIPS.splitlines()
                 if table_name in l or "JOIN" in l or "──" not in l]
    return json.dumps({
        "table": table_name,
        **meta,
        "relationship_hint": "\n".join(rel_lines),
    }, indent=2)


@mcp.tool()
def get_sample_data(table_name: str, n: int = 5) -> str:
    """
    Return n sample rows from a table to understand the data format and values.
    Default is 5 rows. Max 20.
    """
    if table_name not in TABLE_METADATA:
        return json.dumps({"error": f"Unknown table: {table_name}"})
    n = min(n, 20)
    conn = get_db()
    rows = conn.execute(f'SELECT * FROM "{table_name}" LIMIT {n}').fetchall()
    conn.close()
    return json.dumps({
        "table": table_name,
        "sample_rows": [dict(r) for r in rows],
    }, indent=2)


@mcp.tool()
def get_table_relationships() -> str:
    """
    Return the full entity relationship diagram (ERD) as text showing
    how all 8 tables relate to each other with join keys.
    """
    return TABLE_RELATIONSHIPS


@mcp.tool()
def validate_sql(sql: str) -> str:
    """
    Validate a SQL query for syntax correctness using SQLite's EXPLAIN.
    Returns whether the query is valid and any error details.
    Does NOT execute the query — safe to call on any SQL.
    """
    sql = sql.strip()
    conn = get_db()
    try:
        conn.execute(f"EXPLAIN {sql}")
        conn.close()
        return json.dumps({"valid": True, "message": "SQL syntax is valid."})
    except sqlite3.Error as e:
        conn.close()
        return json.dumps({"valid": False, "error": str(e), "sql": sql})


@mcp.tool()
def execute_sql(sql: str) -> str:
    """
    Execute a SQL SELECT query against the payments database and return results.
    Only SELECT queries are allowed for safety. Returns up to 500 rows.
    Always validate_sql before calling this.
    """
    sql = sql.strip()
    # Safety: only allow SELECT
    first_word = re.split(r'\s+', sql)[0].upper()
    if first_word not in ("SELECT", "WITH", "EXPLAIN"):
        return json.dumps({
            "error": "Only SELECT / WITH / EXPLAIN queries are permitted.",
            "sql": sql,
        })
    conn = get_db()
    try:
        cur = conn.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(500)
        conn.close()
        return json.dumps({
            "success": True,
            "row_count": len(rows),
            "columns": cols,
            "rows": [dict(zip(cols, r)) for r in rows],
        }, indent=2)
    except sqlite3.Error as e:
        conn.close()
        return json.dumps({"success": False, "error": str(e), "sql": sql})


@mcp.tool()
def get_column_stats(table_name: str, column_name: str) -> str:
    """
    Get statistics for a numeric column: min, max, avg, count, nulls.
    For text columns returns distinct value counts (top 20).
    Useful for understanding data distribution before writing queries.
    """
    if table_name not in TABLE_METADATA:
        return json.dumps({"error": f"Unknown table: {table_name}"})
    conn = get_db()
    # Detect column type
    pragma = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    col_type = next((r["type"] for r in pragma if r["name"] == column_name), None)
    if col_type is None:
        conn.close()
        return json.dumps({"error": f"Column '{column_name}' not found in '{table_name}'"})

    if col_type in ("REAL", "INTEGER"):
        row = conn.execute(f'''
            SELECT COUNT(*) as total,
                   COUNT("{column_name}") as non_null,
                   MIN("{column_name}") as min_val,
                   MAX("{column_name}") as max_val,
                   AVG("{column_name}") as avg_val,
                   SUM("{column_name}") as sum_val
            FROM "{table_name}"
        ''').fetchone()
        conn.close()
        return json.dumps({"table": table_name, "column": column_name, "type": col_type, **dict(row)}, indent=2)
    else:
        rows = conn.execute(f'''
            SELECT "{column_name}" as value, COUNT(*) as count
            FROM "{table_name}"
            GROUP BY "{column_name}"
            ORDER BY count DESC
            LIMIT 20
        ''').fetchall()
        conn.close()
        return json.dumps({
            "table": table_name, "column": column_name, "type": col_type,
            "top_values": [dict(r) for r in rows]
        }, indent=2)


# ─── RESOURCES ───────────────────────────────────────────────────────────────

@mcp.resource("payments://glossary")
def domain_glossary() -> str:
    """Card payments domain glossary — definitions of key terms."""
    return DOMAIN_GLOSSARY


@mcp.resource("payments://business_rules")
def business_rules() -> str:
    """Business rules, thresholds and SLAs for the card payments domain."""
    return BUSINESS_RULES


@mcp.resource("payments://er_diagram")
def er_diagram() -> str:
    """Entity relationship diagram showing all table joins."""
    return TABLE_RELATIONSHIPS


# ─── FastAPI wrapper with Swagger docs ───────────────────────────────────────
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from starlette.routing import Mount
import uvicorn

def create_app(mcp_port: int = 8001) -> FastAPI:
    """
    Wraps the MCP SSE server inside a FastAPI app so we get:
      /docs      — Swagger UI
      /redoc     — ReDoc UI
      /openapi.json — OpenAPI schema
      /sse       — MCP SSE endpoint (used by the agent)
      /messages/ — MCP message endpoint
    """

    # ── Build OpenAPI tool docs from our metadata ─────────────────────────────
    tool_docs = [
        {
            "name": "list_tables",
            "summary": "List all tables",
            "description": "Returns all 8 tables in the payments database with description, business domain, primary key and current row count.",
            "params": [],
            "returns": "Array of table objects: {table, description, business_domain, primary_key, row_count}",
        },
        {
            "name": "get_table_schema",
            "summary": "Get table schema",
            "description": "Returns full schema for a specific table including column names, SQLite types, nullable flag and business-friendly descriptions.",
            "params": [{"name": "table_name", "type": "string", "required": True, "description": "Name of the table (e.g. 'transactions')"}],
            "returns": "Object: {table, description, business_domain, primary_key, columns[]}",
        },
        {
            "name": "get_table_metadata",
            "summary": "Get table business metadata",
            "description": "Returns rich business context for a table including column meanings, domain description and FK relationship hints.",
            "params": [{"name": "table_name", "type": "string", "required": True, "description": "Name of the table"}],
            "returns": "Object with full metadata including relationship_hint",
        },
        {
            "name": "get_sample_data",
            "summary": "Get sample rows",
            "description": "Returns up to N sample rows from a table so the agent can understand actual data formats and values before writing SQL.",
            "params": [
                {"name": "table_name", "type": "string", "required": True, "description": "Name of the table"},
                {"name": "n", "type": "integer", "required": False, "description": "Number of rows to return (default 5, max 20)"},
            ],
            "returns": "Object: {table, sample_rows[]}",
        },
        {
            "name": "get_table_relationships",
            "summary": "Get full ERD / join map",
            "description": "Returns the complete entity relationship diagram as text — all FK relationships between the 8 tables and suggested join paths.",
            "params": [],
            "returns": "Plain text ERD showing all 1:N relationships and typical join paths",
        },
        {
            "name": "validate_sql",
            "summary": "Validate SQL syntax",
            "description": "Runs SQLite EXPLAIN on the query to check syntax without executing it. Safe to call on any SQL before execution.",
            "params": [{"name": "sql", "type": "string", "required": True, "description": "The SQL query to validate"}],
            "returns": "Object: {valid: bool, message | error}",
        },
        {
            "name": "execute_sql",
            "summary": "Execute a SQL SELECT query",
            "description": "Executes a SELECT/WITH query against the payments SQLite database and returns rows + column names. Only read queries are permitted. Returns up to 500 rows.",
            "params": [{"name": "sql", "type": "string", "required": True, "description": "A valid SELECT or WITH SQL query"}],
            "returns": "Object: {success, row_count, columns[], rows[]}",
        },
        {
            "name": "get_column_stats",
            "summary": "Get column statistics",
            "description": "For numeric columns: returns min, max, avg, sum, null count. For text columns: returns top-20 distinct values with counts. Useful for understanding data distributions before filtering.",
            "params": [
                {"name": "table_name", "type": "string", "required": True, "description": "Name of the table"},
                {"name": "column_name", "type": "string", "required": True, "description": "Name of the column to analyse"},
            ],
            "returns": "Object with stats (numeric) or top_values[] (text)",
        },
    ]

    resource_docs = [
        {"uri": "payments://glossary",      "summary": "Domain glossary",    "description": "Definitions of card payments terminology: authorization, chargeback, interchange, 3DS, etc."},
        {"uri": "payments://business_rules", "summary": "Business rules",     "description": "Thresholds and SLAs: chargeback rate limits, risk score cutoffs, settlement windows, dispute SLAs."},
        {"uri": "payments://er_diagram",     "summary": "ER diagram",         "description": "Full entity relationship diagram showing FK relationships between all 8 tables."},
    ]

    # ── Build OpenAPI paths for each tool ─────────────────────────────────────
    paths = {}

    for tool in tool_docs:
        props = {}
        required = []
        for p in tool["params"]:
            props[p["name"]] = {"type": p["type"], "description": p["description"]}
            if p.get("required"):
                required.append(p["name"])

        request_body = None
        if props:
            request_body = {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": props,
                            "required": required,
                        }
                    }
                },
            }

        paths[f"/tools/{tool['name']}"] = {
            "post": {
                "tags": ["MCP Tools"],
                "summary": tool["summary"],
                "description": f"{tool['description']}\n\n**Returns:** {tool['returns']}",
                "operationId": tool["name"],
                **({"requestBody": request_body} if request_body else {}),
                "responses": {
                    "200": {
                        "description": tool["returns"],
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    }
                },
            }
        }

    for res in resource_docs:
        safe_id = res["uri"].replace("://", "_").replace("/", "_")
        paths[f"/resources/{safe_id}"] = {
            "get": {
                "tags": ["MCP Resources"],
                "summary": res["summary"],
                "description": res["description"],
                "operationId": f"resource_{safe_id}",
                "responses": {
                    "200": {
                        "description": "Plain text content",
                        "content": {"text/plain": {"schema": {"type": "string"}}},
                    }
                },
            }
        }

    # ── Build the custom OpenAPI schema ───────────────────────────────────────
    custom_openapi = {
        "openapi": "3.1.0",
        "info": {
            "title": "Card Payments MCP Server",
            "version": "1.0.0",
            "description": (
                "## Card Payments MCP Server\n\n"
                "MCP (Model Context Protocol) server exposing tools and resources "
                "for querying a Card Payments SQLite database.\n\n"
                "### Database Tables\n"
                "| Table | Domain | Description |\n"
                "|-------|--------|-------------|\n"
                "| `issuers` | Card Issuing | Banks that issue payment cards |\n"
                "| `merchants` | Merchant Mgmt | Businesses accepting card payments |\n"
                "| `cards` | Card Lifecycle | Payment cards issued to customers |\n"
                "| `authorizations` | Auth & Fraud | Real-time auth requests |\n"
                "| `transactions` | Transaction Processing | Settled payment records |\n"
                "| `clearing` | Clearing & Settlement | Batch reconciliation records |\n"
                "| `chargebacks` | Dispute & Chargeback | Disputed transactions |\n"
                "| `dispute_cases` | Customer Disputes | Customer service cases |\n\n"
                "### MCP Endpoints\n"
                "- **SSE connection:** `GET /sse`\n"
                "- **Message endpoint:** `POST /messages/?session_id=...`\n\n"
                "> **Note:** The `/tools/*` and `/resources/*` paths below are "
                "documentation-only. Actual tool calls go through the SSE protocol."
            ),
        },
        "tags": [
            {"name": "MCP Tools",     "description": "Tools the agent can call to explore schema and execute SQL"},
            {"name": "MCP Resources", "description": "Static reference resources: glossary, business rules, ERD"},
            {"name": "MCP Protocol",  "description": "Raw MCP SSE transport endpoints"},
        ],
        "paths": {
            "/sse": {
                "get": {
                    "tags": ["MCP Protocol"],
                    "summary": "MCP SSE connection endpoint",
                    "description": "Establishes a Server-Sent Events connection for MCP communication. This is the endpoint the agent connects to.",
                    "operationId": "mcp_sse_connect",
                    "responses": {"200": {"description": "SSE stream established"}},
                }
            },
            "/messages/": {
                "post": {
                    "tags": ["MCP Protocol"],
                    "summary": "MCP message endpoint",
                    "description": "Receives MCP JSON-RPC messages from the client (agent). Requires an active session_id from the SSE connection.",
                    "operationId": "mcp_post_message",
                    "parameters": [{"name": "session_id", "in": "query", "required": True, "schema": {"type": "string"}}],
                    "responses": {"202": {"description": "Message accepted"}},
                }
            },
            **paths,
        },
    }

    # ── Create FastAPI app with custom OpenAPI ────────────────────────────────
    app = FastAPI(
        title="Card Payments MCP Server",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Override the OpenAPI schema with our custom one
    def custom_openapi_fn():
        return custom_openapi

    app.openapi = custom_openapi_fn

    # ── Mount the MCP SSE Starlette app ───────────────────────────────────────
    mcp.settings.host = "0.0.0.0"
    mcp.settings.port = mcp_port
    starlette_mcp_app = mcp.sse_app()
    app.mount("/", starlette_mcp_app)

    return app


# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8001
    print(f"[MCP] CardPayments MCP Server starting on http://0.0.0.0:{port}")
    print(f"[MCP] Swagger UI  -> http://localhost:{port}/docs")
    print(f"[MCP] ReDoc       -> http://localhost:{port}/redoc")
    print(f"[MCP] SSE endpoint-> http://localhost:{port}/sse")
    app = create_app(mcp_port=port)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")