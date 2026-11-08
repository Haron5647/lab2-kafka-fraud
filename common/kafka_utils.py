"""Вспомогательные функции для работы с Kafka: проверка брокера, создание топиков."""
from __future__ import annotations

import signal
import threading

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import NUM_PARTITIONS, Topics


class BrokerUnavailable(RuntimeError):
    """Брокер Kafka не отвечает."""


def check_broker(bootstrap: str, timeout: float = 5.0) -> None:
    """Убедиться, что Kafka доступна; иначе — понятная ошибка вместо «зависания»."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    try:
        admin.list_topics(timeout=timeout)
    except KafkaException as exc:
        raise BrokerUnavailable(
            f"Kafka недоступна по адресу {bootstrap}. Запущен ли контейнер (docker compose up -d)?"
        ) from exc


def ensure_topics(bootstrap: str, topics: Topics, partitions: int = NUM_PARTITIONS) -> None:
    """Создать недостающие топики (идемпотентно). Нужна, если compose-инициализация не сработала."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    missing = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name in topics.all()
        if name not in existing
    ]
    if not missing:
        return
    for name, future in admin.create_topics(missing, request_timeout=20).items():
        try:
            future.result(timeout=30)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise RuntimeError(f"не удалось создать топик {name}: {exc}") from exc


def install_stop_handlers(stop: threading.Event) -> None:
    """Ctrl+C / SIGTERM -> аккуратная остановка циклов обработки."""

    def _handler(signum, frame):  # noqa: ARG001
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handler)
        except ValueError:  # не главный поток (например, в тестах)
            pass
