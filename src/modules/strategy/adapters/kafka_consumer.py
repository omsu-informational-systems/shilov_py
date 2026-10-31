import json
import logging
import threading
import time
import uuid

from confluent_kafka import Consumer, Producer

from src.config import AppConfig

from ..application.register_service import RegisterServiceUseCase
from ..core.entities import (
    CandleClosedPayload,
    HeartbeatPayload,
    MessageEnvelope,
    RegistrationResultPayload,
)
from ..ports.interfaces import CandleHandlerPort

logger = logging.getLogger("strategy.consumer")


class KafkaConsumerAdapter:
    def __init__(
        self,
        config: AppConfig,
        register_use_case: RegisterServiceUseCase,
        candle_handler: CandleHandlerPort,
    ) -> None:
        self.cfg = config
        self.register_use_case = register_use_case
        self.candle_handler = candle_handler

        self.status = "DOWN"
        self.error_details: str | None = "Not connected"
        self.stop_event = threading.Event()
        self.instance_id = str(uuid.uuid4())
        self.started_at = None

    def _build_producer(self) -> Producer:
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
        return Producer(kafka_cfg)

    def _build_consumer(self, group_suffix: str) -> Consumer:
        kafka_cfg = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
            "group.id": f"{self.cfg.kafka_group_id}.{group_suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
        if self.cfg.kafka_ca_location:
            kafka_cfg["ssl.ca.location"] = self.cfg.kafka_ca_location
        return Consumer(kafka_cfg)

    def _wait_assignment(self, consumer: Consumer, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while not consumer.assignment() and not self.stop_event.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Таймаут назначения партиций")
            consumer.poll(0.2)

    def _send_heartbeat(self, producer: Producer) -> None:
        hb_payload = HeartbeatPayload(
            secretKey=self.cfg.secret_key,
            accountId=self.cfg.account_id,
            groupId=self.cfg.group_id,
            strategyId=self.cfg.strategy_id,
            serviceName=self.cfg.service_name,
            instanceId=self.instance_id,
            status="UP",
            startedAt=self.started_at,
            sentAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            processedCandles=getattr(self.candle_handler, "processed_candles", 0),
            publishedRecommendations=0,
            lastProcessedSequenceNumber=getattr(
                self.candle_handler, "last_sequence_number", None
            ),
            metadata={"appVersion": self.cfg.app_version},
        )
        hb_envelope = MessageEnvelope(
            eventType="account.service.heartbeat",
            producer=self.cfg.service_name,
            payload=hb_payload.to_dict(),
        )
        payload_bytes = json.dumps(hb_envelope.to_dict(), ensure_ascii=False).encode(
            "utf-8"
        )
        producer.produce(
            topic=self.cfg.heartbeat_topic,
            key=self.cfg.service_name.encode("utf-8"),
            value=payload_bytes,
        )
        producer.flush(timeout=1.0)

    def run_loop(self) -> None:
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        while not self.stop_event.is_set():
            producer = None
            reg_consumer = None
            candle_consumer = None
            try:
                logger.info(
                    "event=consumer_connecting bootstrap=%s", self.cfg.bootstrap_servers
                )
                producer = self._build_producer()

                # Готовим чтение ответов регистрации
                reg_consumer = self._build_consumer("reg")
                reg_consumer.subscribe([self.cfg.reg_result_topic])
                self._wait_assignment(reg_consumer)

                # Отправляем запрос регистрации
                reg_envelope = self.register_use_case.execute()

                # Читаем топик ответов и проверяем статус
                registered = False
                rejected = False
                deadline = time.monotonic() + 45

                while not self.stop_event.is_set():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Не получен ответ регистрации за 45 секунд")

                    msg = reg_consumer.poll(0.2)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(
                            f"Ошибка чтения регистрации: {msg.error().code()}"
                        )

                    event = json.loads(msg.value())
                    payload_data = event.get("payload", {})

                    is_own = event.get(
                        "eventType"
                    ) == "account.registration.result" and (
                        event.get("causationId") == reg_envelope.eventId
                        or payload_data.get("accountId") == self.cfg.account_id
                    )

                    if is_own:
                        reg_consumer.commit(message=msg, asynchronous=False)
                        result = RegistrationResultPayload.from_dict(payload_data)

                        if result.status == "REGISTERED":
                            logger.info(
                                'event=registration_confirmed accountId=%s status=%s reason="%s"',
                                result.accountId,
                                result.status,
                                result.reason,
                            )
                            registered = True
                        else:
                            logger.error(
                                'event=registration_rejected accountId=%s status=%s reason="%s"',
                                result.accountId,
                                result.status,
                                result.reason,
                            )
                            self.status = "DOWN"
                            self.error_details = (
                                f"Registration rejected: {result.reason}"
                            )
                            rejected = True
                        break

                if rejected:
                    self.stop_event.wait(30)
                    continue

                if not registered:
                    continue

                # Регистрация успешна -> подписываемся на свечи
                self.status = "UP"
                self.error_details = None

                candle_consumer = self._build_consumer("candles")
                candle_consumer.subscribe([self.cfg.candle_topic])
                self._wait_assignment(candle_consumer)

                logger.info(
                    "event=subscribed_to_candles topic=%s groupId=%s.candles",
                    self.cfg.candle_topic,
                    self.cfg.kafka_group_id,
                )

                # Цикл чтения свечей и отправки Heartbeat
                next_heartbeat = 0.0
                while not self.stop_event.is_set():
                    # Каждые 5 секунд отправляем пульс
                    if time.monotonic() >= next_heartbeat:
                        try:
                            self._send_heartbeat(producer)
                        except Exception as hb_err:  # noqa: BLE001
                            logger.warning("event=heartbeat_failed error=%s", hb_err)
                        next_heartbeat = time.monotonic() + 5.0

                    # Опрашиваем свечи
                    msg = candle_consumer.poll(0.2)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(
                            f"Ошибка чтения свечей: {msg.error().code()}"
                        )

                    event = json.loads(msg.value())
                    if event.get("eventType") == "market.candle.closed":
                        candle_payload = CandleClosedPayload.from_dict(event["payload"])
                        self.candle_handler.handle_candle(
                            event_id=event["eventId"],
                            correlation_id=event.get("correlationId", ""),
                            candle=candle_payload,
                        )

                    candle_consumer.commit(message=msg, asynchronous=False)

            except Exception as exc:  # noqa: BLE001
                self.status = "DOWN"
                self.error_details = type(exc).__name__
                logger.error(
                    'event=kafka_worker_error type=%s details="%s" retryInSec=5',
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
