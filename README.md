# 💳 Card Payments Text2SQL Agent — PoC

A fully agentic Text2SQL system over a synthetic Card Payments database.
Uses **OpenAI gpt-4o-mini** + **MCP (HTTP/SSE)** + **Chainlit** for a
rich chain-of-thought UI.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Chainlit UI  :8000                       │
│   User query → chain-of-thought steps → tool calls → answer    │
└───────────────────────────┬─────────────────────────────────────┘
                            │  async callbacks
┌───────────────────────────▼─────────────────────────────────────┐
│                     Text2SQL Agent                              │
│   OpenAI gpt-4o-mini  ←→  function calling loop                │
│   • Reasons about tables  • Decomposes queries                  │
│   • Validates SQL          • Self-corrects on errors            │
└───────────────────────────┬─────────────────────────────────────┘
                            │  HTTP/SSE
┌───────────────────────────▼─────────────────────────────────────┐
│                 CardPayments MCP Server  :8001                  │
│  Tools:                        Resources:                       │
│  • list_tables                 • payments://glossary            │
│  • get_table_schema            • payments://business_rules      │
│  • get_table_metadata          • payments://er_diagram          │
│  • get_sample_data                                              │
│  • get_table_relationships                                      │
│  • validate_sql                                                 │
│  • execute_sql                                                  │
│  • get_column_stats                                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │  sqlite3
┌───────────────────────────▼─────────────────────────────────────┐
│                   SQLite  data/payments.db                      │
│  8 tables: issuers • merchants • cards • authorizations         │
│            transactions • clearing • chargebacks • disputes     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### 1. Prerequisites
```bash
python >= 3.11
pip
```

### 2. Clone / extract the project
```bash
cd text2sql_poc
```

### 3. Set your OpenAI API key
```bash
export OPENAI_API_KEY=sk-...your-key-here...
```

### 4. Run everything
```bash
chmod +x start.sh
./start.sh
```

This will:
- Install all Python dependencies
- Generate 8 synthetic CSVs (card payments domain data)
- Load them into a local SQLite database
- Start the MCP server on **http://localhost:8001**
- Start the Chainlit UI on **http://localhost:8000**

Open **http://localhost:8000** in your browser.

---

## Project Structure

```
text2sql_poc/
├── app.py                   # Chainlit UI entrypoint
├── start.sh                 # One-command launcher
├── requirements.txt
├── .chainlit/
│   └── config.toml
├── agent/
│   └── agent.py             # Agentic loop (OpenAI + MCP tools)
├── mcp_server/
│   └── server.py            # FastMCP HTTP/SSE server (tools + resources)
├── scripts/
│   ├── generate_data.py     # Synthetic data generator
│   └── load_db.py           # CSV → SQLite loader
└── data/
    ├── csv/                 # 8 generated CSV files
    └── payments.db          # SQLite database (auto-created)
```

---

## Database Schema

| Table | Rows | Description |
|-------|------|-------------|
| `issuers` | 10 | Card-issuing banks |
| `merchants` | 20 | Merchants accepting payments |
| `cards` | 200 | Payment cards issued to customers |
| `authorizations` | 500 | Real-time auth requests |
| `transactions` | 400 | Completed settled transactions |
| `clearing` | 350 | Clearing & reconciliation records |
| `chargebacks` | 100 | Disputed transactions |
| `dispute_cases` | 80 | Customer service dispute cases |

---

## MCP Server Tools

| Tool | Purpose |
|------|---------|
| `list_tables` | See all tables with row counts |
| `get_table_schema` | Column names, types, descriptions |
| `get_table_metadata` | Rich business context per table |
| `get_sample_data` | Sample rows to understand formats |
| `get_table_relationships` | Full ERD with join keys |
| `validate_sql` | Safe SQL syntax check (no execution) |
| `execute_sql` | Run SELECT queries, get results |
| `get_column_stats` | Min/max/avg or value distribution |

---

## Example Questions to Ask

**Basic:**
- What are the top 5 merchants by total transaction volume?
- How many authorizations were declined and what were the reasons?
- List all active corporate cards

**Intermediate:**
- Which issuers have the most chargebacks filed against them?
- Show me transactions that went cross-border at high-risk merchants
- What percentage of disputes have breached SLA?

**Advanced:**
- For each merchant, show total auth amount vs total cleared amount and the difference
- Find cards where the last 3 transactions were all declined
- Which chargeback reason codes have the highest merchant win rate?

---

## Configuration

| Env Variable | Default | Purpose |
|-------------|---------|---------|
| `OPENAI_API_KEY` | _(required)_ | Your OpenAI key |
| `MCP_SERVER_URL` | `http://localhost:8001/sse` | MCP server endpoint |

---

## How the Agent Works

1. **Receives** the user's natural language question
2. **Calls `list_tables`** to orient itself (or goes direct if context is clear)
3. **Calls `get_table_schema`** / `get_table_metadata` for relevant tables
4. **Checks `get_table_relationships`** to plan JOINs
5. **Optionally calls `get_sample_data`** or `get_column_stats` to understand values
6. **Generates SQL** based on gathered context
7. **Calls `validate_sql`** — if invalid, fixes and retries
8. **Calls `execute_sql`** to run the query
9. **Returns results** with a business-friendly explanation

All steps are streamed live in the Chainlit UI as expandable steps.
