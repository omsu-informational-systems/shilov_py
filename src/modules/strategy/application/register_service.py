import logging

from src.config import AppConfig

from ..core.entities import MessageEnvelope, RegistrationRequestPayload
from ..ports.interfaces import MessagePublisherPort

logger = logging.getLogger("strategy.registration")


class RegisterServiceUseCase:
    def __init__(self, publisher: MessagePublisherPort, config: AppConfig) -> None:
        self.publisher = publisher
        self.config = config

    def execute(self) -> MessageEnvelope:
        """Формирует и отправляет событие account.registration.request."""
        payload = RegistrationRequestPayload(
            secretKey=self.config.secret_key,  # Передается КЛЮЧ пары согласно обновленной документации
            accountId=self.config.account_id,
            accountName=self.config.account_name,
            groupId=self.config.group_id,
            strategyId=self.config.strategy_id,
            strategyName="Momentum Strategy",
            serviceName=self.config.service_name,
            description="Импульсная стратегия студента",
        )

        envelope = MessageEnvelope(
            eventType="account.registration.request",
            producer=self.config.service_name,
            payload=payload.to_dict(),
        )

        logger.info(
            "event=sending_registration eventId=%s accountId=%s groupId=%s strategyId=%s",
            envelope.eventId,
            self.config.account_id,
            self.config.group_id,
            self.config.strategy_id,
        )

        self.publisher.publish(
            topic=self.config.reg_request_topic,
            key=self.config.service_name,
            envelope=envelope,
        )

        return envelope
