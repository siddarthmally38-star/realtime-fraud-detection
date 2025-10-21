"""
Fraud detection rule engine — evaluates transactions against configurable
rules and produces a weighted fraud score.

Each rule returns a score between 0.0 (no risk) and 1.0 (high risk),
weighted by its importance. The final fraud score is the weighted sum,
clamped to [0, 1].
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import structlog

from config.settings import settings

logger = structlog.get_logger()


class TransactionHistory(Protocol):
    """Interface for accessing customer transaction history."""

    def get_recent_count(self, customer_id: str, window_minutes: int) -> int: ...
    def get_recent_small_count(
        self, customer_id: str, amount_threshold: float, window_minutes: int
    ) -> int: ...
    def get_avg_amount(self, customer_id: str) -> float: ...
    def get_known_countries(self, customer_id: str) -> set[str]: ...


@dataclass
class RuleResult:
    rule_name: str
    score: float  # 0.0 to 1.0
    weight: float
    reason: str


def rule_high_amount(txn: dict, history: TransactionHistory) -> RuleResult:
    """Flag transactions with unusually high amounts."""
    amount = txn["amount"]
    threshold = settings.fraud.high_amount_threshold
    customer_avg = history.get_avg_amount(txn["customer_id"])

    score = 0.0
    reason = ""

    if amount > threshold:
        score = min(1.0, amount / (threshold * 2))
        reason = f"Amount ${amount:.2f} exceeds threshold ${threshold:.2f}"
    elif customer_avg > 0 and amount > customer_avg * 3:
        score = min(1.0, (amount / customer_avg - 1) / 5)
        reason = f"Amount ${amount:.2f} is {amount/customer_avg:.1f}x customer average"

    return RuleResult(
        rule_name="high_amount",
        score=score,
        weight=0.3,
        reason=reason,
    )


def rule_velocity(txn: dict, history: TransactionHistory) -> RuleResult:
    """Flag rapid successive transactions (velocity check)."""
    window = settings.fraud.velocity_window_minutes
    max_count = settings.fraud.velocity_max_count
    recent_count = history.get_recent_count(txn["customer_id"], window)

    score = 0.0
    reason = ""

    if recent_count >= max_count:
        score = min(1.0, recent_count / (max_count * 2))
        reason = f"{recent_count} transactions in {window} minutes (max: {max_count})"

    return RuleResult(
        rule_name="velocity",
        score=score,
        weight=0.25,
        reason=reason,
    )


def rule_geo_anomaly(txn: dict, history: TransactionHistory) -> RuleResult:
    """Flag transactions from unknown countries."""
    country = txn.get("country", "US")
    known_countries = history.get_known_countries(txn["customer_id"])

    score = 0.0
    reason = ""

    if known_countries and country not in known_countries:
        # Higher risk for countries with higher fraud rates
        high_risk_countries = {"NG", "RU", "CN", "BR", "IN", "PH"}
        if country in high_risk_countries:
            score = 0.9
        else:
            score = 0.5
        reason = f"Transaction from {country}, known countries: {known_countries}"

    return RuleResult(
        rule_name="geo_anomaly",
        score=score,
        weight=0.2,
        reason=reason,
    )


def rule_card_testing(txn: dict, history: TransactionHistory) -> RuleResult:
    """Flag card testing patterns (many small transactions)."""
    amount = txn["amount"]
    threshold = settings.fraud.card_test_amount_threshold
    window = settings.fraud.card_test_window_minutes
    min_count = settings.fraud.card_test_min_count

    score = 0.0
    reason = ""

    if amount < threshold:
        small_count = history.get_recent_small_count(
            txn["customer_id"], threshold, window
        )
        if small_count >= min_count:
            score = min(1.0, small_count / (min_count * 2))
            reason = f"{small_count} small transactions (<${threshold}) in {window} minutes"

    return RuleResult(
        rule_name="card_testing",
        score=score,
        weight=0.15,
        reason=reason,
    )


def rule_time_anomaly(txn: dict, history: TransactionHistory) -> RuleResult:
    """Flag transactions during unusual hours (2-5 AM local time)."""
    try:
        ts = datetime.fromisoformat(txn["timestamp"].replace("Z", "+00:00"))
        hour = ts.hour
    except (ValueError, KeyError):
        return RuleResult("time_anomaly", 0.0, 0.1, "")

    score = 0.0
    reason = ""

    if 2 <= hour <= 5:
        score = 0.6
        reason = f"Transaction at {hour}:00 UTC (unusual hours)"

    return RuleResult(
        rule_name="time_anomaly",
        score=score,
        weight=0.1,
        reason=reason,
    )


ALL_RULES = [
    rule_high_amount,
    rule_velocity,
    rule_geo_anomaly,
    rule_card_testing,
    rule_time_anomaly,
]


def score_transaction(txn: dict, history: TransactionHistory) -> dict:
    """
    Score a transaction against all fraud rules.

    Returns the transaction enriched with:
    - fraud_score: weighted score [0, 1]
    - is_fraud: boolean flag if score >= threshold
    - rule_results: list of individual rule results
    """
    results = [rule(txn, history) for rule in ALL_RULES]

    total_weight = sum(r.weight for r in results)
    fraud_score = sum(r.score * r.weight for r in results) / max(total_weight, 0.01)
    fraud_score = max(0.0, min(1.0, fraud_score))

    triggered_rules = [
        {"rule": r.rule_name, "score": round(r.score, 4), "reason": r.reason}
        for r in results
        if r.score > 0
    ]

    return {
        **txn,
        "fraud_score": round(fraud_score, 4),
        "is_fraud": fraud_score >= settings.fraud.score_threshold,
        "triggered_rules": triggered_rules,
        "rules_fired": len(triggered_rules),
    }
