"""Общая конфигурация проекта: «контракт» между тремя частями системы.

Здесь лежат адрес Kafka, имена топиков и пороги правил. Менять эти значения
нужно только по согласованию всех участников команды.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# Адрес брокера. Для запуска из docker-compose с хоста это localhost:9092.
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Число партиций у каждого топика. Ключ сообщений — card_id, поэтому все операции
# одной карты попадают в одну партицию и обрабатываются строго по порядку.
NUM_PARTITIONS = 3


@dataclass(frozen=True)
class Topics:
    """Имена топиков. Префикс нужен сквозным тестам, чтобы не трогать рабочие топики."""

    transactions: str = "transactions"  # входной поток от генератора
    legit: str = "legit-transactions"  # канал легитимных операций
    fraud: str = "fraud-alerts"  # канал реагирования на угрозы

    @classmethod
    def with_prefix(cls, prefix: str) -> "Topics":
        base = cls()
        return cls(prefix + base.transactions, prefix + base.legit, prefix + base.fraud)

    def all(self) -> list[str]:
        return [self.transactions, self.legit, self.fraud]


DEFAULT_TOPICS = Topics()

# --- Правило 1: слишком частые операции по одной карте -----------------------
BURST_WINDOW_SECONDS = 30.0  # длина окна
BURST_MAX_TRANSACTIONS = 3  # «более 3 транзакций» за окно -> блокировка на 4-й

# --- Правило 2: новый город + аномально большая сумма ------------------------
GEO_AMOUNT_MULTIPLIER = 10.0  # сумма >= 10 x средней
MIN_HISTORY_FOR_GEO_RULE = 3  # сколько операций нужно, чтобы «средний чек» был осмысленным
MAX_TRACKED_CITIES = 10  # сколько городов помнить по каждой карте (память ограничена)
