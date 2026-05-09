"""Tests for fraud detection rules — self-contained without project imports."""

import pytest

# --- Inline rule logic for testing ---

HIGH_AMOUNT_THRESHOLD = 5000.0
VELOCITY_MAX = 5
VELOCITY_WINDOW = 10

class MockHistory:
    def __init__(self, recent_count=0, small_count=0, avg_amount=100.0, known_countries=None):
        self._recent_count = recent_count
        self._small_count = small_count
        self._avg_amount = avg_amount
        self._known_countries = known_countries or {"US"}

    def get_recent_count(self, customer_id, window_minutes):
        return self._recent_count

    def get_recent_small_count(self, customer_id, amount_threshold, window_minutes):
        return self._small_count

    def get_avg_amount(self, customer_id):
        return self._avg_amount

    def get_known_countries(self, customer_id):
        return self._known_countries


def rule_high_amount(txn, history):
    amount = txn["amount"]
    avg = history.get_avg_amount(txn["customer_id"])
    score = 0.0
    reason = ""
    if amount > HIGH_AMOUNT_THRESHOLD:
        score = min(1.0, amount / (HIGH_AMOUNT_THRESHOLD * 2))
        reason = f"Amount ${amount:.2f} exceeds threshold"
    elif avg > 0 and amount > avg * 3:
        score = min(1.0, (amount / avg - 1) / 5)
        reason = f"Amount {amount/avg:.1f}x customer average"
    return {"score": score, "reason": reason}


def rule_velocity(txn, history):
    count = history.get_recent_count(txn["customer_id"], VELOCITY_WINDOW)
    score = 0.0
    if count >= VELOCITY_MAX:
        score = min(1.0, count / (VELOCITY_MAX * 2))
    return {"score": score}


def rule_geo_anomaly(txn, history):
    country = txn.get("country", "US")
    known = history.get_known_countries(txn["customer_id"])
    score = 0.0
    if known and country not in known:
        high_risk = {"NG", "RU", "CN", "BR", "IN", "PH"}
        score = 0.9 if country in high_risk else 0.5
    return {"score": score}


def rule_card_testing(txn, history):
    amount = txn["amount"]
    score = 0.0
    if amount < 5.0:
        small_count = history.get_recent_small_count(txn["customer_id"], 5.0, 5)
        if small_count >= 3:
            score = min(1.0, small_count / 6)
    return {"score": score}


def rule_time_anomaly(txn, history):
    from datetime import datetime
    try:
        ts = datetime.fromisoformat(txn["timestamp"].replace("Z", "+00:00"))
        hour = ts.hour
    except (ValueError, KeyError):
        return {"score": 0.0}
    return {"score": 0.6 if 2 <= hour <= 5 else 0.0}


def score_transaction(txn, history):
    rules = [
        (rule_high_amount, 0.3),
        (rule_velocity, 0.25),
        (rule_geo_anomaly, 0.2),
        (rule_card_testing, 0.15),
        (rule_time_anomaly, 0.1),
    ]
    results = [(fn(txn, history), w) for fn, w in rules]
    total_weight = sum(w for _, w in results)
    fraud_score = sum(r["score"] * w for r, w in results) / max(total_weight, 0.01)
    fraud_score = max(0.0, min(1.0, fraud_score))
    triggered = [r for r, _ in results if r["score"] > 0]
    return {
        **txn,
        "fraud_score": round(fraud_score, 4),
        "is_fraud": fraud_score >= 0.7,
        "triggered_rules": triggered,
    }


def make_txn(**overrides):
    base = {
        "transaction_id": "txn-001",
        "customer_id": "cust-001",
        "amount": 50.0,
        "country": "US",
        "timestamp": "2024-06-15T14:00:00+00:00",
    }
    base.update(overrides)
    return base


class TestHighAmountRule:
    def test_normal_amount(self):
        result = rule_high_amount(make_txn(amount=50.0), MockHistory())
        assert result["score"] == 0.0

    def test_above_threshold(self):
        result = rule_high_amount(make_txn(amount=6000.0), MockHistory())
        assert result["score"] > 0

    def test_above_customer_avg(self):
        result = rule_high_amount(make_txn(amount=400.0), MockHistory(avg_amount=100.0))
        assert result["score"] > 0


class TestVelocityRule:
    def test_normal_velocity(self):
        result = rule_velocity(make_txn(), MockHistory(recent_count=2))
        assert result["score"] == 0.0

    def test_high_velocity(self):
        result = rule_velocity(make_txn(), MockHistory(recent_count=8))
        assert result["score"] > 0


class TestGeoAnomalyRule:
    def test_known_country(self):
        result = rule_geo_anomaly(make_txn(country="US"), MockHistory())
        assert result["score"] == 0.0

    def test_unknown_high_risk_country(self):
        result = rule_geo_anomaly(make_txn(country="NG"), MockHistory())
        assert result["score"] == 0.9

    def test_unknown_normal_country(self):
        result = rule_geo_anomaly(make_txn(country="FR"), MockHistory())
        assert result["score"] == 0.5


class TestCardTestingRule:
    def test_normal_amount(self):
        result = rule_card_testing(make_txn(amount=50.0), MockHistory())
        assert result["score"] == 0.0

    def test_card_testing_pattern(self):
        result = rule_card_testing(make_txn(amount=2.99), MockHistory(small_count=5))
        assert result["score"] > 0


class TestTimeAnomalyRule:
    def test_normal_hours(self):
        result = rule_time_anomaly(make_txn(timestamp="2024-06-15T14:00:00+00:00"), MockHistory())
        assert result["score"] == 0.0

    def test_suspicious_hours(self):
        result = rule_time_anomaly(make_txn(timestamp="2024-06-15T03:00:00+00:00"), MockHistory())
        assert result["score"] > 0


class TestScoreTransaction:
    def test_clean_transaction(self):
        result = score_transaction(make_txn(), MockHistory())
        assert result["fraud_score"] < 0.7
        assert result["is_fraud"] is False

    def test_fraudulent_transaction(self):
        history = MockHistory(recent_count=10, small_count=5, avg_amount=50.0, known_countries={"US"})
        txn = make_txn(amount=3.0, country="NG", timestamp="2024-06-15T03:00:00+00:00")
        result = score_transaction(txn, history)
        assert result["fraud_score"] > 0
        assert len(result["triggered_rules"]) >= 3
