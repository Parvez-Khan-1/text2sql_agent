"""
Text2SQL Agent
  - Connects to the CardPayments MCP server via HTTP/SSE
  - Uses OpenAI gpt-4o-mini with function calling
  - Fully autonomous: discovers schema, reasons, generates SQL, validates, executes
  - Streams thinking steps back via callbacks so Chainlit can show chain-of-thought
"""

import json
import os
from typing import Callable, Any
from pathlib import Path
from openai import AsyncOpenAI
from mcp import ClientSession
from mcp.client.sse import sse_client

# ── Load .env file if present (Windows-friendly alternative to export) ────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8001/sse")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
MODEL = "gpt-4o-mini"

if not OPENAI_API_KEY:
    raise EnvironmentError(
        "OPENAI_API_KEY is not set. "
        "Run: export OPENAI_API_KEY=sk-... or add it to your .env file."
    )

SYSTEM_PROMPT = """You are a Text2SQL Agent for a Card Payments database. Your sole purpose is to answer questions about the data in this database by generating and executing SQL queries.

SCOPE — you ONLY handle:
- Questions about data in the database (transactions, merchants, cards, authorizations, chargebacks, clearing, disputes, issuers)
- SQL generation, explanation and execution
- Schema and data exploration
- Business insights derived from the data

OUT OF SCOPE — if the user asks anything unrelated to the database or card payments data (e.g. general knowledge, coding help, personal questions, opinions, anything not answerable from the data), respond with exactly:
"I can only answer questions about the Card Payments database. Please ask me something about the data."

Do not engage with out-of-scope questions at all — just return that message.

For in-scope questions, your job is to:
1. UNDERSTAND the user's question in the card payments business context
2. EXPLORE the database using the available tools (list_tables, get_table_schema, get_table_metadata, etc.)
3. REASON about which tables and joins are needed
4. GENERATE the optimal SQL query
5. VALIDATE the SQL using validate_sql before executing
6. EXECUTE using execute_sql and return clear, formatted results
7. EXPLAIN the results in business terms

AGENT RULES:
- Always start by listing tables if you're unsure which tables are involved
- Always check schema before writing SQL for unfamiliar tables
- Always validate SQL before executing — fix any errors and retry
- Use get_sample_data to understand data formats when needed
- Use get_column_stats to understand value distributions for filter conditions
- Read get_table_relationships to plan multi-table joins
- For complex questions, decompose into sub-queries, then combine
- If a query returns 0 rows, investigate why (check filters, column values)
- Always present results in a clear table or summary format
- Explain what the SQL does and what the results mean business-wise
- NEVER add implicit filters (e.g. is_active = 1, status = 'APPROVED') unless the user explicitly asks for them — always query ALL rows unless told otherwise
- When counting unique records, use COUNT(*) on the full table unless a filter is explicitly requested

IMPORTANT SQL RULES for SQLite:
- Use double quotes for identifiers: "table_name"."column_name"
- Use single quotes for string values: WHERE status = 'APPROVED'
- SQLite does not have TOP, use LIMIT instead
- Date comparisons: WHERE date_col >= '2024-01-01'
- Boolean columns stored as 0/1: WHERE is_active = 1
- Use ROUND() for decimal formatting

Think step-by-step, reason out loud, and be thorough.
"""


class Text2SQLAgent:
    def __init__(self):
        self.client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        self.mcp_tools: list[dict] = []

    async def _load_mcp_tools(self, session: ClientSession):
        """Convert MCP tools to OpenAI function-call format."""
        tools_response = await session.list_tools()
        self.mcp_tools = []
        for tool in tools_response.tools:
            self.mcp_tools.append({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or "",
                    "parameters": tool.inputSchema if tool.inputSchema else {
                        "type": "object", "properties": {}
                    },
                },
            })

    async def _call_mcp_tool(self, session: ClientSession, name: str, args: dict) -> str:
        """Call an MCP tool and return string result."""
        result = await session.call_tool(name, args)
        if result.content:
            return "\n".join(
                c.text if hasattr(c, "text") else str(c)
                for c in result.content
            )
        return "No result returned."

    async def run(
        self,
        user_query: str,
        on_thinking: Callable[[str], Any] | None = None,
        on_tool_call: Callable[[str, dict], Any] | None = None,
        on_tool_result: Callable[[str, str], Any] | None = None,
    ) -> str:
        """
        Run the full agentic loop for a user query.
        Callbacks let Chainlit stream each step live.
        """
        async with sse_client(MCP_SERVER_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                await self._load_mcp_tools(session)

                messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_query},
                ]

                iteration = 0
                max_iterations = 20

                while iteration < max_iterations:
                    iteration += 1

                    response = await self.client.chat.completions.create(
                        model=MODEL,
                        messages=messages,
                        tools=self.mcp_tools if self.mcp_tools else None,
                        tool_choice="auto",
                        temperature=0.1,
                    )

                    msg = response.choices[0].message
                    finish = response.choices[0].finish_reason

                    # Stream any text thinking
                    if msg.content:
                        if on_thinking:
                            await on_thinking(msg.content)

                    # If no tool calls → final answer
                    if finish == "stop" or not msg.tool_calls:
                        return msg.content or "Done."

                    # Append assistant message with tool calls
                    messages.append({
                        "role": "assistant",
                        "content": msg.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in msg.tool_calls
                        ],
                    })

                    # Execute each tool call
                    for tc in msg.tool_calls:
                        tool_name = tc.function.name
                        try:
                            args = json.loads(tc.function.arguments)
                        except Exception:
                            args = {}

                        if on_tool_call:
                            await on_tool_call(tool_name, args)

                        result = await self._call_mcp_tool(session, tool_name, args)

                        if on_tool_result:
                            await on_tool_result(tool_name, result)

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result,
                        })

                return "Agent reached max iterations without a final answer."