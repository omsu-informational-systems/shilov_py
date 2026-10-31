from abc import ABC, abstractmethod

from ..core.entities import CandleClosedPayload, MessageEnvelope


class MessagePublisherPort(ABC):
    @abstractmethod
    def publish(self, topic: str, key: str, envelope: MessageEnvelope) -> None:
        pass


class CandleHandlerPort(ABC):
    @abstractmethod
    def handle_candle(
        self, event_id: str, correlation_id: str, candle: CandleClosedPayload
    ) -> None:
        """Обработка полученной свечи."""
