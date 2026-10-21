import json
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from src.config import AppConfig

from ..application.process_candle import ProcessCandleUseCase
from ..core.entities import CandleEvent, RecommendationDecision
from ..ports.interfaces import DeduplicationStorePort, RecommendationPublisherPort


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def make_envelope(
    kind: str,
    service: str,
    payload: dict,
    correlation: str | None = None,
    cause: str | None = None,
) -> dict:
    event_id = str(uuid.uuid4())
    return {
        "eventId": event_id,
        "eventType": kind,
        "schemaVersion": "1.0",
        "occurredAt": utc_now(),
        "correlationId": correlation or event_id,
        "causationId": cause,
        "producer": service,
        "payload": payload,
    }


class InMemoryDeduplicationStore(DeduplicationStorePort):
    def __init__(self, max_size: int = 4096):
        self.seen: OrderedDict[str, bool] = OrderedDict()
        self.max_size = max_size

    def is_seen(self, event_id: str) -> bool:
        return event_id in self.seen

    def mark_seen(self, event_id: str) -> None:
        self.seen[event_id] = True
        if len(self.seen) > self.max_size:
            self.seen.popitem(last=False)


class KafkaAdapter(RecommendationPublisherPort):
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.status = "STARTING"
        self.error_details: str | None = "initializing"
        self.stop_event = threading.Event()
        self.started_at = utc_now()
        self.instance_id = str(uuid.uuid4())

        kafka_cfg = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
        }
        if self.cfg.kafka_ca_location:
            kafka_cfg["ssl.ca.location"] = self.cfg.kafka_ca_location

        self.producer = Producer({**kafka_cfg, "enable.idempotence": True})
        common_consumer = {
            **kafka_cfg,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "earliest",
        }
        self.answers = Consumer(
            {**common_consumer, "group.id": f"{self.cfg.kafka_group_id}.answers"}
        )
        self.candles = Consumer(
            {**common_consumer, "group.id": f"{self.cfg.kafka_group_id}.candles"}
        )

    def _publish_raw(self, topic: str, key: str, event: dict) -> None:
        delivered = []
        self.producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(event, ensure_ascii=False).encode(),
            on_delivery=lambda err, msg: delivered.append(err),
        )
        remaining = self.producer.flush(5)
        if remaining or not delivered or delivered[0] is not None:
            raise RuntimeError("Не подтверждена публикация в Kafka")

    def publish_recommendation(self, decision: RecommendationDecision) -> None:
        event = make_envelope(
            kind="strategy.recommendation.created",
            service=self.cfg.service_name,
            payload={
                "recommendationId": decision.recommendation_id,
                "sourceEventId": decision.source_event_id,
                "runId": decision.run_id,
                "secretKey": self.cfg.secret_key,
                "accountId": self.cfg.account_id,
                "groupId": self.cfg.group_id,
                "strategyId": self.cfg.strategy_id,
                "instrument": decision.instrument,
                "sequenceNumber": decision.sequence_number,
                "signal": decision.signal.value,
                "confidence": decision.confidence,
                "suggestedQuantity": decision.suggested_quantity,
                "dataQuality": decision.data_quality,
                "reason": decision.reason,
                "processingTimeMs": decision.processing_time_ms,
                "metadata": decision.metadata,
            },
            correlation=decision.correlation_id,
            cause=decision.source_event_id,
        )
        self._publish_raw(
            "strategy.recommendation.created", decision.source_event_id, event
        )

    def _read_event(self, consumer: Consumer, timeout: float = 0.0):
        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Ошибка Kafka: {msg.error().code()}")
        if msg.value() is None:
            raise ValueError("Пустое сообщение")
        return msg, json.loads(msg.value())

    def _wait_assignment(self, consumer: Consumer) -> None:
        deadline = time.monotonic() + 30
        while not consumer.assignment() and not self.stop_event.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Не назначены разделы темы ответов")
            consumer.poll(0.2)

    def run_loop(self, use_case: ProcessCandleUseCase) -> None:
        try:
            metadata = self.producer.list_topics(timeout=10)
            expected = {
                self.cfg.candle_topic,
                "account.registration.request",
                "account.registration.result",
                "account.service.heartbeat",
                "strategy.recommendation.created",
                "strategy.recommendation.result",
            }
            if any(
                t not in metadata.topics or metadata.topics[t].error for t in expected
            ):
                raise ValueError("Нужная тема отсутствует на брокере")

            self.answers.subscribe(
                ["account.registration.result", "strategy.recommendation.result"]
            )
            self._wait_assignment(self.answers)

            identity_payload = {
                "secretKey": self.cfg.secret_key,
                "accountId": self.cfg.account_id,
                "accountName": self.cfg.account_name,
                "groupId": self.cfg.group_id,
                "strategyId": self.cfg.strategy_id,
                "strategyName": "Momentum Strategy",
                "serviceName": self.cfg.service_name,
                "description": "Сервис импульсной стратегии",
            }
            reg_event = make_envelope(
                "account.registration.request", self.cfg.service_name, identity_payload
            )
            self._publish_raw(
                "account.registration.request", self.cfg.service_name, reg_event
            )

            deadline = time.monotonic() + 30
            while not self.stop_event.is_set():
                if time.monotonic() > deadline:
                    raise TimeoutError("Таймаут ожидания ответа регистрации")
                received = self._read_event(self.answers, 0.2)
                if received is None:
                    continue
                msg, event = received
                is_own = (
                    event.get("eventType") == "account.registration.result"
                    and event.get("causationId") == reg_event["eventId"]
                )
                self.answers.commit(message=msg, asynchronous=False)
                if is_own:
                    if event["payload"].get("status") != "REGISTERED":
                        raise RuntimeError("Регистрация отклонена платформой")
                    break

            self.status = "UP"
            self.error_details = None
            print(f"Регистрация подтверждена. Читаю тему: {self.cfg.candle_topic}")

            self.candles.subscribe([self.cfg.candle_topic])
            next_heartbeat = 0.0

            while not self.stop_event.is_set():
                if time.monotonic() >= next_heartbeat:
                    hb = make_envelope(
                        "account.service.heartbeat",
                        self.cfg.service_name,
                        {
                            "secretKey": self.cfg.secret_key,
                            "accountId": self.cfg.account_id,
                            "groupId": self.cfg.group_id,
                            "strategyId": self.cfg.strategy_id,
                            "serviceName": self.cfg.service_name,
                            "instanceId": self.instance_id,
                            "status": "UP",
                            "startedAt": self.started_at,
                            "sentAt": utc_now(),
                            "processedCandles": use_case.processed_candles,
                            "publishedRecommendations": use_case.published_recommendations,
                            "lastProcessedSequenceNumber": use_case.last_sequence_number,
                            "metadata": {},
                        },
                    )
                    self._publish_raw(
                        "account.service.heartbeat", self.cfg.service_name, hb
                    )
                    next_heartbeat = time.monotonic() + 5

                ans_rec = self._read_event(self.answers, 0.0)
                if ans_rec is not None:
                    msg, event = ans_rec
                    payload = event.get("payload", {})
                    if (
                        event.get("eventType") == "strategy.recommendation.result"
                        and payload.get("accountId") == self.cfg.account_id
                        and payload.get("strategyId") == self.cfg.strategy_id
                    ):
                        print(
                            "Результат:",
                            payload.get("instrument"),
                            payload.get("sequenceNumber"),
                            payload.get("accountExecution", {}).get("status"),
                        )
                    self.answers.commit(message=msg, asynchronous=False)

                cnd_rec = self._read_event(self.candles, 0.1)
                if cnd_rec is None:
                    continue
                msg, event = cnd_rec
                if event.get("eventType") != "market.candle.closed":
                    self.candles.commit(message=msg, asynchronous=False)
                    continue

                payload = event["payload"]
                candle_entity = CandleEvent(
                    event_id=event["eventId"],
                    correlation_id=event["correlationId"],
                    run_id=payload["runId"],
                    ticker=payload["instrument"]["ticker"],
                    sequence_number=payload["sequenceNumber"],
                    close_price=str(payload.get("close", "0")),
                )
                use_case.execute(candle_entity)
                self.candles.commit(message=msg, asynchronous=False)

        except Exception as e:
            self.status = "DOWN"
            self.error_details = str(e)
            print(f"Ошибка Kafka-воркера: {e}")
        finally:
            self.candles.close()
            self.answers.close()
            self.producer.flush(5)
