from abc import ABC, abstractmethod

from ..core.entities import CandleClosedEvent


class CandleHandlerPort(ABC):
    @abstractmethod
    def handle_candle(self, candle: CandleClosedEvent) -> None:
        pass
