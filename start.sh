#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start.sh — Launch MCP server + Chainlit agent (Windows/Mac/Linux compatible)
# ─────────────────────────────────────────────────────────────────────────────
set -e

cd "$(dirname "$0")"

# Force UTF-8 output on Windows
export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1

# ── Load .env if present ─────────────────────────────────────────────────────
if [ -f ".env" ]; then
  echo "[setup] Loading .env file..."
  export $(grep -v '^#' .env | xargs)
fi

# ── Check env vars ────────────────────────────────────────────────────────────
if [ -z "$OPENAI_API_KEY" ]; then
  echo "ERROR: OPENAI_API_KEY is not set."
  echo "  Option 1 — export in shell:  export OPENAI_API_KEY=sk-..."
  echo "  Option 2 — create a .env file (copy .env.example and fill in your key)"
  exit 1
fi

export MCP_SERVER_URL="${MCP_SERVER_URL:-http://localhost:8001/sse}"

# ── Install deps ──────────────────────────────────────────────────────────────
echo "[setup] Installing dependencies..."
pip install -q -r requirements.txt

# ── Delete stale Chainlit config (let it auto-regenerate) ────────────────────
if [ -f ".chainlit/config.toml" ]; then
  echo "[setup] Removing stale Chainlit config (will regenerate)..."
  rm ".chainlit/config.toml"
fi

# ── Generate data if not present ─────────────────────────────────────────────
if [ ! -f "data/payments.db" ]; then
  echo "[setup] Generating synthetic data..."
  python scripts/generate_data.py
  python scripts/load_db.py
else
  echo "[setup] SQLite DB already exists, skipping data generation"
fi

# ── Start MCP server in background ───────────────────────────────────────────
echo "[mcp] Starting MCP Server on port 8001..."
python -u mcp_server/server.py 8001 &
MCP_PID=$!
echo "[mcp] PID: $MCP_PID"

# Wait for MCP server to be ready
echo "[mcp] Waiting for MCP server to be ready..."
for i in $(seq 1 10); do
  sleep 1
  if curl -s --max-time 1 "http://localhost:8001/sse" > /dev/null 2>&1; then
    echo "[mcp] Server is up!"
    break
  fi
  echo "[mcp] ...waiting ($i/10)"
done

# ── Start Chainlit ────────────────────────────────────────────────────────────
echo ""
echo "[chainlit] Starting UI on http://localhost:8000"
echo "[chainlit] Press Ctrl+C to stop"
echo ""
chainlit run app.py --port 8000 --host 0.0.0.0

# ── Cleanup on exit ───────────────────────────────────────────────────────────
trap "echo 'Stopping MCP server...'; kill $MCP_PID 2>/dev/null" EXIT