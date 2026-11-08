"""Модели данных: транзакция и тревога. Формат обмена через Kafka — JSON (UTF-8)."""
from __future__ import annotations

import json
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any

# Названия правил, которые попадают в поле Alert.rule
RULE_RATE_LIMIT = "rate_limit"  # правило 1: больше 3 операций за 30 секунд
RULE_GEO_AMOUNT = "geo_amount"  # правило 2: новый город и сумма >= 10 x средней
RULE_CARD_BLOCKED = "card_blocked"  # карта уже заблокирована раньше

ACTION_BLOCK_CARD = "BLOCK_CARD"


class InvalidTransaction(ValueError):
    """Сообщение нельзя разобрать как транзакцию (или как тревогу)."""


def _number(data: dict[str, Any], name: str) -> float:
    value = data.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidTransaction(f"поле {name!r} должно быть конечным числом, получено {value!r}")
    return float(value)


def _text(data: dict[str, Any], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value.strip():
        raise InvalidTransaction(f"поле {name!r} должно быть непустой строкой, получено {value!r}")
    return value


@dataclass(frozen=True)
class Transaction:
    """Одна банковская операция.

    ``timestamp`` — время события в секундах Unix. Именно оно, а не время прихода
    сообщения, используется правилами: так поведение детерминировано и тестируется.
    ``scenario`` — диагностическая метка генератора («legit», «fraud_burst»,
    «fraud_spike»). Детектор её НЕ использует, она нужна только для проверки результатов.
    """

    tx_id: str
    card_id: str
    amount: float
    city: str
    category: str
    timestamp: float
    scenario: str = "legit"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Any) -> "Transaction":
        if not isinstance(data, dict):
            raise InvalidTransaction("сообщение должно быть JSON-объектом")
        amount = _number(data, "amount")
        if amount <= 0:
            raise InvalidTransaction(f"сумма должна быть положительной, получено {amount}")
        return cls(
            tx_id=_text(data, "tx_id"),
            card_id=_text(data, "card_id"),
            amount=amount,
            city=_text(data, "city"),
            category=_text(data, "category"),
            timestamp=_number(data, "timestamp"),
            scenario=str(data.get("scenario", "legit")),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Transaction":
        try:
            data = json.loads(raw)
        except ValueError as exc:  # JSONDecodeError и UnicodeDecodeError
            raise InvalidTransaction(f"некорректный JSON: {exc}") from exc
        return cls.from_dict(data)


@dataclass(frozen=True)
class Alert:
    """Тревога: какая транзакция, каким правилом признана подозрительной и что делать."""

    rule: str
    reason: str
    transaction: Transaction
    action: str = ACTION_BLOCK_CARD
    detected_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "reason": self.reason,
            "action": self.action,
            "detected_at": self.detected_at,
            "transaction": self.transaction.to_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Alert":
        try:
            data = json.loads(raw)
        except ValueError as exc:
            raise InvalidTransaction(f"некорректный JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise InvalidTransaction("тревога должна быть JSON-объектом")
        return cls(
            rule=_text(data, "rule"),
            reason=_text(data, "reason"),
            transaction=Transaction.from_dict(data.get("transaction")),
            action=str(data.get("action", ACTION_BLOCK_CARD)),
            detected_at=_number(data, "detected_at"),
        )
