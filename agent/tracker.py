"""
Token usage tracker for the Text2SQL agent.
Tracks per-question and cumulative session usage + cost.
"""

from dataclasses import dataclass, field
from typing import Optional

# GPT-4o-mini pricing (USD per token)
PRICING = {
    "gpt-4o-mini": {
        "input":  0.150 / 1_000_000,
        "output": 0.600 / 1_000_000,
    },
    "gpt-4o": {
        "input":  2.50 / 1_000_000,
        "output": 10.00 / 1_000_000,
    },
    "gpt-4-turbo": {
        "input":  10.00 / 1_000_000,
        "output": 30.00 / 1_000_000,
    },
}


@dataclass
class TurnUsage:
    """Usage stats for a single LLM call (one iteration of the agentic loop)."""
    turn: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    input_cost_usd: float
    output_cost_usd: float
    total_cost_usd: float
    tool_calls_made: list[str] = field(default_factory=list)


@dataclass
class QuestionUsage:
    """Aggregated usage for one user question (may span many LLM turns)."""
    question: str
    turns: list[TurnUsage] = field(default_factory=list)

    @property
    def total_prompt_tokens(self) -> int:
        return sum(t.prompt_tokens for t in self.turns)

    @property
    def total_completion_tokens(self) -> int:
        return sum(t.completion_tokens for t in self.turns)

    @property
    def total_tokens(self) -> int:
        return sum(t.total_tokens for t in self.turns)

    @property
    def total_cost_usd(self) -> float:
        return sum(t.total_cost_usd for t in self.turns)

    @property
    def llm_calls(self) -> int:
        return len(self.turns)

    @property
    def all_tool_calls(self) -> list[str]:
        tools = []
        for t in self.turns:
            tools.extend(t.tool_calls_made)
        return tools


class UsageTracker:
    """
    Session-level tracker. One instance lives per Chainlit session.
    Accumulates across all questions asked in the session.
    """

    def __init__(self, model: str = "gpt-4o-mini"):
        self.model = model
        self.pricing = PRICING.get(model, PRICING["gpt-4o-mini"])
        self.history: list[QuestionUsage] = []
        self._current: Optional[QuestionUsage] = None

    # ── Question lifecycle ────────────────────────────────────────────────────

    def start_question(self, question: str):
        self._current = QuestionUsage(question=question[:80])

    def end_question(self) -> Optional[QuestionUsage]:
        if self._current:
            self.history.append(self._current)
        q = self._current
        self._current = None
        return q

    # ── Per-turn recording ────────────────────────────────────────────────────

    def record_turn(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        tool_calls: list[str] | None = None,
    ) -> TurnUsage:
        input_cost  = prompt_tokens     * self.pricing["input"]
        output_cost = completion_tokens * self.pricing["output"]
        turn = TurnUsage(
            turn              = len(self._current.turns) + 1 if self._current else 0,
            prompt_tokens     = prompt_tokens,
            completion_tokens = completion_tokens,
            total_tokens      = prompt_tokens + completion_tokens,
            input_cost_usd    = input_cost,
            output_cost_usd   = output_cost,
            total_cost_usd    = input_cost + output_cost,
            tool_calls_made   = tool_calls or [],
        )
        if self._current:
            self._current.turns.append(turn)
        return turn

    # ── Session aggregates ────────────────────────────────────────────────────

    @property
    def session_total_tokens(self) -> int:
        return sum(q.total_tokens for q in self.history)

    @property
    def session_total_cost(self) -> float:
        return sum(q.total_cost_usd for q in self.history)

    @property
    def session_llm_calls(self) -> int:
        return sum(q.llm_calls for q in self.history)

    @property
    def session_questions(self) -> int:
        return len(self.history)

    # ── Formatters ────────────────────────────────────────────────────────────

    def format_question_summary(self, q: QuestionUsage) -> str:
        tool_counts: dict[str, int] = {}
        for t in q.all_tool_calls:
            tool_counts[t] = tool_counts.get(t, 0) + 1

        tool_str = ", ".join(f"`{k}` ×{v}" for k, v in tool_counts.items()) or "none"

        rows = [
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| LLM calls | **{q.llm_calls}** |",
            f"| Prompt tokens | {q.total_prompt_tokens:,} |",
            f"| Completion tokens | {q.total_completion_tokens:,} |",
            f"| Total tokens | **{q.total_tokens:,}** |",
            f"| Estimated cost | **${q.total_cost_usd:.6f}** |",
            f"| Tools called | {tool_str} |",
        ]

        if len(q.turns) > 1:
            rows.append("")
            rows.append("**Turn breakdown:**")
            rows.append("| Turn | Prompt | Completion | Cost | Tools |")
            rows.append("|------|--------|------------|------|-------|")
            for t in q.turns:
                tools = ", ".join(f"`{x}`" for x in t.tool_calls_made) or "—"
                rows.append(
                    f"| {t.turn} | {t.prompt_tokens:,} | {t.completion_tokens:,} "
                    f"| ${t.total_cost_usd:.6f} | {tools} |"
                )

        return "\n".join(rows)

    def format_session_summary(self) -> str:
        if not self.history:
            return "_No questions asked yet._"

        rows = [
            "| Metric | Value |",
            "|--------|-------|",
            f"| Questions answered | **{self.session_questions}** |",
            f"| Total LLM calls | **{self.session_llm_calls}** |",
            f"| Total tokens used | **{self.session_total_tokens:,}** |",
            f"| Total session cost | **${self.session_total_cost:.6f}** |",
            f"| Avg cost per question | **${self.session_total_cost / max(self.session_questions,1):.6f}** |",
            f"| Model | `{self.model}` |",
        ]

        if self.history:
            rows += [
                "",
                "**Per-question breakdown:**",
                "| # | Question | LLM Calls | Tokens | Cost |",
                "|---|----------|-----------|--------|------|",
            ]
            for i, q in enumerate(self.history, 1):
                short_q = q.question[:40] + ("..." if len(q.question) > 40 else "")
                rows.append(
                    f"| {i} | {short_q} | {q.llm_calls} | {q.total_tokens:,} | ${q.total_cost_usd:.6f} |"
                )

        return "\n".join(rows)