"""
Chainlit UI for the Text2SQL Agent
Shows full chain-of-thought: thinking steps, tool calls, tool results, final answer
"""

import os
import json
import chainlit as cl
from agent.agent import Text2SQLAgent, MODEL
from agent.tracker import UsageTracker

TOOL_ICONS = {
    "list_tables":            "📋",
    "get_table_schema":       "🏗️",
    "get_table_metadata":     "📖",
    "get_sample_data":        "🔍",
    "get_table_relationships":"🔗",
    "validate_sql":           "✅",
    "execute_sql":            "⚡",
    "get_column_stats":       "📊",
}

TOOL_LABELS = {
    "list_tables":            "Listing all tables",
    "get_table_schema":       "Fetching schema",
    "get_table_metadata":     "Reading metadata",
    "get_sample_data":        "Sampling data",
    "get_table_relationships":"Checking relationships",
    "validate_sql":           "Validating SQL",
    "execute_sql":            "Executing SQL",
    "get_column_stats":       "Analysing column stats",
}

OUT_OF_SCOPE_KEYWORDS = [
    "weather", "recipe", "cook", "movie", "sport", "news", "joke",
    "poem", "story", "stock price", "crypto", "translate", "write an email",
    "who is", "what is the capital", "tell me about yourself", "how are you",
    "meaning of life", "recommend a book", "play a game",
]

def is_out_of_scope(text: str) -> bool:
    lowered = text.lower()
    # Allow anything that mentions SQL, data, table or payment domain terms
    data_signals = [
        "sql", "query", "table", "database", "select", "data", "count",
        "merchant", "transaction", "card", "authorization", "chargeback",
        "clearing", "dispute", "issuer", "payment", "amount", "how many",
        "show me", "list", "find", "what are", "which", "top", "average",
        "total", "sum", "group", "filter", "where", "join",
    ]
    if any(kw in lowered for kw in data_signals):
        return False
    if any(kw in lowered for kw in OUT_OF_SCOPE_KEYWORDS):
        return True
    # If very short and no data signal, let the agent decide
    return False

OUT_OF_SCOPE_REPLY = (
    "I'm the **Text2SQL Agent** for the Card Payments database. "
    "I can only answer questions about the data — transactions, merchants, "
    "cards, authorizations, chargebacks, clearing and disputes.\n\n"
    "Try asking something like:\n"
    "- *What are the top merchants by transaction volume?*\n"
    "- *How many chargebacks are still open?*\n"
    "- *Show me all declined authorizations due to fraud.*"
)


def format_tool_result(tool_name: str, result: str, args: dict | None = None) -> str:
    """Pretty-format tool results for display."""
    try:
        data = json.loads(result)
    except Exception:
        return f"```\n{result[:1500]}\n```"

    if tool_name == "list_tables" and isinstance(data, list):
        lines = ["| Table | Domain | Rows |", "|-------|--------|------|"]
        for t in data:
            lines.append(f"| **{t['table']}** | {t['business_domain']} | {t['row_count']:,} |")
        return "\n".join(lines)

    if tool_name == "execute_sql" and isinstance(data, dict):
        parts = []
        if args and args.get("sql"):
            parts.append(f"**Generated SQL:**\n```sql\n{args['sql']}\n```")
        if not data.get("success"):
            parts.append(f"**Error:** `{data.get('error', 'unknown')}`")
            return "\n\n".join(parts)
        rows = data.get("rows", [])
        cols = data.get("columns", [])
        if not rows:
            parts.append("_Query returned 0 rows._")
            return "\n\n".join(parts)
        lines = ["| " + " | ".join(cols) + " |",
                 "| " + " | ".join(["---"] * len(cols)) + " |"]
        for row in rows[:30]:
            lines.append("| " + " | ".join(str(row.get(c, "")) for c in cols) + " |")
        if len(rows) > 30:
            lines.append(f"\n_...and {len(rows)-30} more rows_")
        parts.append(f"**{data['row_count']} row(s) returned**\n\n" + "\n".join(lines))
        return "\n\n".join(parts)

    if tool_name == "validate_sql" and isinstance(data, dict):
        parts = []
        if args and args.get("sql"):
            parts.append(f"**SQL being validated:**\n```sql\n{args['sql']}\n```")
        if data.get("valid"):
            parts.append("✅ SQL is syntactically valid.")
        else:
            parts.append(f"❌ Invalid SQL: `{data.get('error', '')}`")
        return "\n\n".join(parts)

    if tool_name == "get_table_schema" and isinstance(data, dict):
        cols = data.get("columns", [])
        lines = [f"**{data['table']}** — {data.get('description','')}",
                 "",
                 "| Column | Type | Description |",
                 "|--------|------|-------------|"]
        for c in cols:
            lines.append(f"| `{c['column']}` | {c['type']} | {c.get('description','')} |")
        return "\n".join(lines)

    raw = json.dumps(data, indent=2)
    if len(raw) > 2000:
        raw = raw[:2000] + "\n... (truncated)"
    return f"```json\n{raw}\n```"


@cl.on_chat_start
async def on_start():
    # Create a fresh tracker for this session
    cl.user_session.set("tracker", UsageTracker(model=MODEL))

    await cl.Message(
        content=(
            "## Text2SQL Agent\n\n"
            "Ask a question about the Card Payments database and I will generate "
            "the SQL, execute it, and explain the results.\n\n"
            "**Available data:** transactions, merchants, cards, authorizations, "
            "chargebacks, clearing, disputes, issuers.\n\n"
            "Type `/cost` at any time to see your session token usage and cost summary."
        )
    ).send()


@cl.on_message
async def on_message(message: cl.Message):

    # Handle /cost command
    if message.content.strip().lower() in ("/cost", "/usage"):
        tracker: UsageTracker = cl.user_session.get("tracker")
        await cl.Message(
            content=f"## Session Token Usage & Cost\n\n{tracker.format_session_summary()}",
            author="Usage Tracker"
        ).send()
        return

    # Scope guard — reject off-topic questions immediately
    if is_out_of_scope(message.content):
        await cl.Message(content=OUT_OF_SCOPE_REPLY).send()
        return

    tracker: UsageTracker = cl.user_session.get("tracker")
    agent = Text2SQLAgent(tracker=tracker)

    thinking_msg: cl.Message | None = None
    thinking_buf: list[str] = []
    step_count = [0]

    async def on_thinking(text: str):
        nonlocal thinking_msg
        thinking_buf.append(text)
        if thinking_msg is None:
            thinking_msg = cl.Message(content="", author="Agent Thinking")
            await thinking_msg.send()
        thinking_msg.content = "\n\n".join(thinking_buf)
        await thinking_msg.update()

    async def on_tool_call(tool_name: str, args: dict):
        step_count[0] += 1
        icon = TOOL_ICONS.get(tool_name, "🔧")
        label = TOOL_LABELS.get(tool_name, tool_name)
        if tool_name in ("execute_sql", "validate_sql") and "sql" in args:
            arg_str = f"```sql\n{args['sql']}\n```"
        else:
            arg_str = ", ".join(f"`{k}={v}`" for k, v in args.items()) if args else "_no arguments_"
        async with cl.Step(name=f"{icon} {label}", type="tool") as step:
            step.input = arg_str
            cl.user_session.set(f"step_{tool_name}_{step_count[0]}", step)
            cl.user_session.set(f"args_{tool_name}_{step_count[0]}", args)

    async def on_tool_result(tool_name: str, result: str):
        key = f"step_{tool_name}_{step_count[0]}"
        args_key = f"args_{tool_name}_{step_count[0]}"
        step: cl.Step | None = cl.user_session.get(key)
        args: dict = cl.user_session.get(args_key) or {}
        formatted = format_tool_result(tool_name, result, args)
        if step:
            step.output = formatted
            await step.update()
        else:
            await cl.Message(
                content=formatted,
                author=f"{TOOL_ICONS.get(tool_name, '🔧')} {tool_name}"
            ).send()

    async def on_turn_usage(turn_usage):
        # Update the live usage badge in the step sidebar
        pass  # summary shown after final answer instead

    final = await agent.run(
        user_query=message.content,
        on_thinking=on_thinking,
        on_tool_call=on_tool_call,
        on_tool_result=on_tool_result,
        on_turn_usage=on_turn_usage,
    )

    # Final answer
    await cl.Message(
        content=f"## Answer\n\n{final}",
        author="Text2SQL Agent"
    ).send()

    # Show per-question usage summary after every answer
    last_q = tracker.history[-1] if tracker.history else None
    if last_q:
        await cl.Message(
            content=(
                f"### Token Usage — this question\n\n"
                f"{tracker.format_question_summary(last_q)}\n\n"
                f"---\n"
                f"_Session total: **{tracker.session_total_tokens:,} tokens** — "
                f"**${tracker.session_total_cost:.6f}** across "
                f"**{tracker.session_questions}** question(s) / "
                f"**{tracker.session_llm_calls}** LLM call(s). "
                f"Type `/cost` for full session breakdown._"
            ),
            author="Usage Tracker"
        ).send()