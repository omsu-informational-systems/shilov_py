import json
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from confluent_kafka import Consumer, Producer

from src.config import AppConfig

from ..application.use_cases import ProcessCandleUseCase
from ..core.entities import Candle, Recommendation
from ..ports.interfaces import RecommendationPublisherPort


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


class KafkaAdapter(RecommendationPublisherPort):
    def __init__(self, config: AppConfig):
        self.cfg = config
        self.status = "DOWN"
        self.error_details: str | None = "Инициализация подключения к брокеру"
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
            raise RuntimeError("Не подтверждена публикация сообщения в Kafka")

    def publish_recommendation(self, rec: Recommendation) -> None:
        event = make_envelope(
            kind="strategy.recommendation.created",
            service=self.cfg.service_name,
            payload={
                "recommendationId": rec.recommendation_id,
                "sourceEventId": rec.source_event_id,
                "runId": rec.run_id,
                "secretKey": self.cfg.secret_key,
                "strategyId": self.cfg.strategy_id,
                "accountId": self.cfg.account_id,
                "groupId": self.cfg.group_id,
                "instrument": rec.instrument,
                "sequenceNumber": rec.sequence_number,
                "signal": rec.signal.value,
                "confidence": rec.confidence,
                "suggestedQuantity": rec.suggested_quantity,
                "dataQuality": rec.data_quality.value,
                "reason": rec.reason,
                "processingTimeMs": rec.processing_time_ms,
                "metadata": rec.metadata,
            },
            correlation=rec.correlation_id,
            cause=rec.source_event_id,
        )
        self._publish_raw("strategy.recommendation.created", rec.source_event_id, event)
        # Безопасное логирование без secretKey (Глава 10)
        print(
            f"[RECOMMENDATION] runId={rec.run_id} ticker={rec.instrument} "
            f"seq={rec.sequence_number} signal={rec.signal.value} "
            f"qty={rec.suggested_quantity} conf={rec.confidence}"
        )

    def _read_event(self, consumer: Consumer, timeout: float = 0.0):
        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Код ошибки клиента Kafka: {msg.error().code()}")
        if msg.value() is None:
            raise ValueError("Получено пустое сообщение из Kafka")
        return msg, json.loads(msg.value())

    def _wait_assignment(self, consumer: Consumer) -> None:
        deadline = time.monotonic() + 30
        while not consumer.assignment() and not self.stop_event.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Таймаут назначения разделов темы ответов")
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
                raise ValueError("Одна из обязательных тем отсутствует на брокере")

            self.answers.subscribe(
                ["account.registration.result", "strategy.recommendation.result"]
            )
            self._wait_assignment(self.answers)

            reg_payload = {
                "secretKey": self.cfg.secret_key,
                "accountId": self.cfg.account_id,
                "accountName": self.cfg.account_name,
                "groupId": self.cfg.group_id,
                "strategyId": self.cfg.strategy_id,
                "strategyName": "Momentum Strategy",
                "serviceName": self.cfg.service_name,
                "description": "Импульсная торговая стратегия (DDD)",
            }
            reg_event = make_envelope(
                "account.registration.request", self.cfg.service_name, reg_payload
            )
            self._publish_raw(
                "account.registration.request", self.cfg.service_name, reg_event
            )

            deadline = time.monotonic() + 30
            while not self.stop_event.is_set():
                if time.monotonic() > deadline:
                    raise TimeoutError("Не получен ответ регистрации за 30 секунд")
                received = self._read_event(self.answers, 0.2)
                if received is None:
                    continue
                msg, event = received
                payload = event.get("payload", {})
                # Проверяем causationId и свои идентификаторы (Глава 6)
                is_own = (
                    event.get("eventType") == "account.registration.result"
                    and event.get("causationId") == reg_event["eventId"]
                    and payload.get("accountId") == self.cfg.account_id
                    and payload.get("serviceName") == self.cfg.service_name
                )
                self.answers.commit(message=msg, asynchronous=False)
                if is_own:
                    if payload.get("status") != "REGISTERED":
                        raise RuntimeError("Регистрация отклонена платформой")
                    break

            self.status = "UP"
            self.error_details = None
            print(
                f"[REGISTERED] accountId={self.cfg.account_id} topic={self.cfg.candle_topic}"
            )

            self.candles.subscribe([self.cfg.candle_topic])
            next_heartbeat = 0.0

            while not self.stop_event.is_set():
                # 1. Отправка сигнала состояния каждые 5 секунд (Глава 6)
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
                            "metadata": {"appVersion": self.cfg.app_version},
                        },
                    )
                    self._publish_raw(
                        "account.service.heartbeat", self.cfg.service_name, hb
                    )
                    next_heartbeat = time.monotonic() + 5

                # 2. Чтение результатов исполнения рекомендаций (Глава 9)
                ans_rec = self._read_event(self.answers, 0.0)
                if ans_rec is not None:
                    msg, event = ans_rec
                    payload = event.get("payload", {})
                    if (
                        event.get("eventType") == "strategy.recommendation.result"
                        and payload.get("accountId") == self.cfg.account_id
                        and payload.get("strategyId") == self.cfg.strategy_id
                    ):
                        ticker = payload.get("instrument")
                        portfolio = payload.get("accountPortfolio", {})
                        exec_info = payload.get("accountExecution", {})
                        if ticker and "positionQuantity" in portfolio:
                            use_case.handle_execution_result(
                                ticker, int(portfolio["positionQuantity"])
                            )
                        print(
                            f"[RESULT] ticker={ticker} seq={payload.get('sequenceNumber')} "
                            f"execStatus={exec_info.get('status')} pos={portfolio.get('positionQuantity')}"
                        )
                    self.answers.commit(message=msg, asynchronous=False)

                # 3. Чтение закрытых свечей (Глава 7)
                cnd_rec = self._read_event(self.candles, 0.1)
                if cnd_rec is None:
                    continue
                msg, event = cnd_rec
                if event.get("eventType") != "market.candle.closed":
                    self.candles.commit(message=msg, asynchronous=False)
                    continue

                p = event["payload"]
                inst = p.get("instrument", {})
                # У старых сообщений sessionPhase может отсутствовать (Глава 7)
                candle_entity = Candle(
                    event_id=event["eventId"],
                    correlation_id=event["correlationId"],
                    run_id=p["runId"],
                    ticker=inst["ticker"],
                    lot_size=int(inst.get("lotSize", 1)),
                    timeframe=p.get("timeframe", "1m"),
                    sequence_number=int(p["sequenceNumber"]),
                    open_price=Decimal(str(p["open"])),
                    high_price=Decimal(str(p["high"])),
                    low_price=Decimal(str(p["low"])),
                    close_price=Decimal(str(p["close"])),
                    volume=int(p.get("volume", 0)),
                    session_phase=p.get("sessionPhase", "MAIN_SESSION"),
                    is_warmup=bool(p.get("isWarmup", False)),
                )
                use_case.execute(candle_entity)
                self.candles.commit(message=msg, asynchronous=False)

        except Exception as e:
            self.status = "DOWN"
            # Не выводим чувствительные данные в details (Глава 10)
            self.error_details = type(e).__name__
            print(f"[ERROR] Ошибка цикла Kafka: {type(e).__name__}: {e}")
        finally:
            self.candles.close()
            self.answers.close()
            self.producer.flush(5)
