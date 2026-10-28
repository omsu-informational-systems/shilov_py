import json
import logging
import threading
import time
import uuid
from dataclasses import asdict

from confluent_kafka import Consumer, Producer

from src.config import AppConfig

from ..core.entities import (
    CandleClosedEvent,
    MessageEnvelope,
    RegistrationRequestPayload,
    utc_now,
)
from ..ports.interfaces import CandleHandlerPort

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("strategy.kafka")


class KafkaWorkerAdapter:
    def __init__(self, config: AppConfig, candle_handler: CandleHandlerPort) -> None:
        self.cfg = config
        self.candle_handler = candle_handler
        self.status = "DOWN"
        self.error_details: str | None = "Not connected"
        self.stop_event = threading.Event()
        self.instance_id = str(uuid.uuid4())
        self.started_at = utc_now()

    def _create_clients(self) -> tuple[Producer, Consumer, Consumer]:
        base_conf = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
        }
        producer = Producer({**base_conf, "enable.idempotence": True})
        consumer_conf = {
            **base_conf,
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "earliest",
        }
        reg_consumer = Consumer(
            {**consumer_conf, "group.id": f"{self.cfg.kafka_group_id}.reg"}
        )
        candle_consumer = Consumer(
            {**consumer_conf, "group.id": f"{self.cfg.kafka_group_id}.candles"}
        )
        return producer, reg_consumer, candle_consumer

    def _send(
        self, producer: Producer, topic: str, key: str, envelope: MessageEnvelope
    ) -> None:
        delivered = []
        payload_bytes = json.dumps(envelope.to_dict(), ensure_ascii=False).encode(
            "utf-8"
        )
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=payload_bytes,
            on_delivery=lambda err, msg: delivered.append(err),
        )
        remaining = producer.flush(5)
        if remaining or not delivered or delivered[0] is not None:
            raise RuntimeError(f"Не удалось доставить сообщение в топик {topic}")

    def _wait_partition_assignment(self, consumer: Consumer) -> None:
        deadline = time.monotonic() + 30
        while not consumer.assignment() and not self.stop_event.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Таймаут ожидания назначения партиций")
            consumer.poll(0.2)

    def run(self) -> None:
        # Цикл переподключения: сбой брокера не завершает процесс сервиса (Пункт 9)
        while not self.stop_event.is_set():
            producer = None
            reg_consumer = None
            candle_consumer = None
            try:
                logger.info(
                    "event=kafka_connecting bootstrap=%s", self.cfg.bootstrap_servers
                )
                producer, reg_consumer, candle_consumer = self._create_clients()

                # Проверяем доступность брокера
                producer.list_topics(timeout=10)

                # 1. Подписываемся на ответы регистрации ДО отправки запроса
                reg_consumer.subscribe([self.cfg.reg_result_topic])
                self._wait_partition_assignment(reg_consumer)

                # 2. Отправляем запрос регистрации (Пункт 6)
                reg_payload = RegistrationRequestPayload(
                    secretKey=self.cfg.secret_key,
                    accountId=self.cfg.account_id,
                    accountName=self.cfg.account_name,
                    groupId=self.cfg.group_id,
                    strategyId=self.cfg.strategy_id,
                    strategyName="Momentum Strategy",
                    serviceName=self.cfg.service_name,
                    description="Подключение и чтение свечей",
                )
                reg_envelope = MessageEnvelope(
                    eventType="account.registration.request",
                    producer=self.cfg.service_name,
                    payload=asdict(reg_payload),
                )
                self._send(
                    producer,
                    self.cfg.reg_request_topic,
                    self.cfg.service_name,
                    reg_envelope,
                )
                logger.info(
                    "event=registration_request_sent eventId=%s accountId=%s groupId=%s strategyId=%s",
                    reg_envelope.eventId,
                    self.cfg.account_id,
                    self.cfg.group_id,
                    self.cfg.strategy_id,
                )

                # 3. Ждем результат регистрации (Пункт 7)
                registered = False
                rejected = False
                deadline = time.monotonic() + 30

                while not self.stop_event.is_set():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Не получен ответ регистрации за 30 секунд")
                    msg = reg_consumer.poll(0.2)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(
                            f"Ошибка чтения ответа регистрации: {msg.error().code()}"
                        )

                    event = json.loads(msg.value())
                    reg_consumer.commit(message=msg, asynchronous=False)

                    if (
                        event.get("eventType") == "account.registration.result"
                        and event.get("causationId") == reg_envelope.eventId
                    ):
                        payload = event.get("payload", {})
                        status = payload.get("status")
                        reason = payload.get("reason", "No reason provided")

                        if status == "REGISTERED":
                            logger.info(
                                'event=registration_accepted accountId=%s serviceName=%s reason="%s"',
                                self.cfg.account_id,
                                self.cfg.service_name,
                                reason,
                            )
                            registered = True
                        else:
                            logger.error(
                                'event=registration_rejected accountId=%s status=%s reason="%s"',
                                self.cfg.account_id,
                                status,
                                reason,
                            )
                            self.status = "DOWN"
                            self.error_details = f"Registration rejected: {reason}"
                            rejected = True
                        break

                if rejected:
                    # При отказе в регистрации не читаем свечи и не публикуем ответы (Пункт 7)
                    self.stop_event.wait(30)
                    continue

                if not registered:
                    continue

                # 4. Подписываемся на топик свечей (Пункт 8)
                self.status = "UP"
                self.error_details = None
                candle_consumer.subscribe([self.cfg.candle_topic])
                logger.info(
                    "event=candle_subscription_active topic=%s groupId=%s.candles",
                    self.cfg.candle_topic,
                    self.cfg.kafka_group_id,
                )

                next_heartbeat = 0.0
                while not self.stop_event.is_set():
                    # Отправляем Heartbeat каждые 5 секунд, чтобы платформа видела статус ONLINE
                    if time.monotonic() >= next_heartbeat:
                        hb_envelope = MessageEnvelope(
                            eventType="account.service.heartbeat",
                            producer=self.cfg.service_name,
                            payload={
                                "secretKey": self.cfg.secret_key,
                                "accountId": self.cfg.account_id,
                                "groupId": self.cfg.group_id,
                                "strategyId": self.cfg.strategy_id,
                                "serviceName": self.cfg.service_name,
                                "instanceId": self.instance_id,
                                "status": "UP",
                                "startedAt": self.started_at,
                                "sentAt": utc_now(),
                                "processedCandles": getattr(
                                    self.candle_handler, "processed_candles", 0
                                ),
                                "publishedRecommendations": 0,
                                "lastProcessedSequenceNumber": getattr(
                                    self.candle_handler, "last_sequence_number", None
                                ),
                                "metadata": {"appVersion": self.cfg.app_version},
                            },
                        )
                        self._send(
                            producer,
                            self.cfg.heartbeat_topic,
                            self.cfg.service_name,
                            hb_envelope,
                        )
                        next_heartbeat = time.monotonic() + 5

                    # Читаем свечи
                    msg = candle_consumer.poll(0.2)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(
                            f"Ошибка чтения топика свечей: {msg.error().code()}"
                        )

                    event = json.loads(msg.value())
                    if event.get("eventType") == "market.candle.closed":
                        p = event["payload"]
                        candle = CandleClosedEvent(
                            event_id=event["eventId"],
                            correlation_id=event["correlationId"],
                            run_id=p["runId"],
                            ticker=p["instrument"]["ticker"],
                            timeframe=p.get("timeframe", "1m"),
                            sequence_number=int(p["sequenceNumber"]),
                            candle_time=p.get("candleTime", ""),
                            open_price=str(p["open"]),
                            high_price=str(p["high"]),
                            low_price=str(p["low"]),
                            close_price=str(p["close"]),
                            volume=int(p.get("volume", 0)),
                        )
                        self.candle_handler.handle_candle(candle)

                    candle_consumer.commit(message=msg, asynchronous=False)

            except Exception as exc:
                self.status = "DOWN"
                self.error_details = type(exc).__name__
                logger.error(
                    'event=kafka_connection_error error=%s details="%s" retryInSec=5',
                    type(exc).__name__,
                    exc,
                )
            finally:
                if candle_consumer:
                    candle_consumer.close()
                if reg_consumer:
                    reg_consumer.close()
                if producer:
                    producer.flush(2)

            if not self.stop_event.is_set():
                self.stop_event.wait(5)
