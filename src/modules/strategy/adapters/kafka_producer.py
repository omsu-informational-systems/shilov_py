import json
import logging

from confluent_kafka import Producer

from src.config import AppConfig

from ..core.entities import MessageEnvelope
from ..ports.interfaces import MessagePublisherPort

logger = logging.getLogger("strategy.producer")


class KafkaProducerAdapter(MessagePublisherPort):
    def __init__(self, config: AppConfig) -> None:
        self.cfg = config
        kafka_cfg = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
            "enable.idempotence": True,
        }
        if self.cfg.kafka_ca_location:
            kafka_cfg["ssl.ca.location"] = self.cfg.kafka_ca_location

        self.producer = Producer(kafka_cfg)

    def publish(self, topic: str, key: str, envelope: MessageEnvelope) -> None:
        delivered_errors: list[Exception | None] = []

        def _delivery_report(err, msg):
            if err is not None:
                delivered_errors.append(err)

        payload_bytes = json.dumps(envelope.to_dict(), ensure_ascii=False).encode(
            "utf-8"
        )

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload_bytes,
            on_delivery=_delivery_report,
        )

        # Ожидаем подтверждения от брокера до 5 секунд
        remaining = self.producer.flush(timeout=5.0)

        if remaining > 0 or delivered_errors:
            error_msg = delivered_errors[0] if delivered_errors else "таймаут доставки"
            raise RuntimeError(
                f"Не удалось доставить сообщение в топик {topic}: {error_msg}"
            )

        logger.info(
            "event=message_published topic=%s eventType=%s eventId=%s",
            topic,
            envelope.eventType,
            envelope.eventId,
        )

    def close(self) -> None:
        self.producer.flush(timeout=2.0)
