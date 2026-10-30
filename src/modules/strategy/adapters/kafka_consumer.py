import json
import logging
import threading
import time

from confluent_kafka import Consumer

from src.config import AppConfig

from ..application.register_service import RegisterServiceUseCase
from ..core.entities import CandleClosedPayload, RegistrationResultPayload
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

    def _build_consumer(self, group_suffix: str) -> Consumer:
        kafka_cfg = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
            # Уникальный group.id для студента (Пункт 8)
            "group.id": f"{self.cfg.kafka_group_id}.{group_suffix}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        }
        if self.cfg.kafka_ca_location:
            kafka_cfg["ssl.ca.location"] = self.cfg.kafka_ca_location

        return Consumer(kafka_cfg)

    def _wait_assignment(self, consumer: Consumer, timeout: float = 30.0) -> None:
        """Ожидание назначения партиций топика (требование главы 4)."""
        deadline = time.monotonic() + timeout
        while not consumer.assignment() and not self.stop_event.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Таймаут назначения партиций")
            consumer.poll(0.2)

    def run_loop(self) -> None:
        # Внешний цикл: при сбое связи сервис пробует снова через 5 секунд (Пункт 9)
        while not self.stop_event.is_set():
            reg_consumer = None
            candle_consumer = None
            try:
                logger.info(
                    "event=consumer_connecting bootstrap=%s", self.cfg.bootstrap_servers
                )

                # 1. Готовим чтение ответов регистрации
                reg_consumer = self._build_consumer("reg")
                reg_consumer.subscribe([self.cfg.reg_result_topic])
                self._wait_assignment(reg_consumer)

                # 2. Отправляем запрос регистрации
                reg_envelope = self.register_use_case.execute()

                # 3. Читаем топик ответов и проверяем статус (Пункт 7)
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
                            f"Ошибка Kafka при чтении регистрации: {msg.error().code()}"
                        )

                    event = json.loads(msg.value())
                    payload_data = event.get("payload", {})

                    # Проверяем наш ли это ответ
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
                            # При отказе пишем причину и не идем к чтению свечей (Пункт 7)
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
                    # При отказе в ключе ждем 30 секунд
                    self.stop_event.wait(30)
                    continue

                if not registered:
                    continue

                # 4. Регистрация успешна -> подписываемся на свечи (Пункт 8)
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

                # 5. Цикл чтения свечей
                while not self.stop_event.is_set():
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

                    # Ручной коммит смещения
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

            if not self.stop_event.is_set():
                self.stop_event.wait(5)
