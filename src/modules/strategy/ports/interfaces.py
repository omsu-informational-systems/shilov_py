from abc import ABC, abstractmethod

from ..core.entities import Recommendation, TickerState


class RecommendationPublisherPort(ABC):
    @abstractmethod
    def publish_recommendation(self, recommendation: Recommendation) -> None:
        pass


class StrategyStateRepositoryPort(ABC):
    @abstractmethod
    def is_candle_processed(self, event_id: str) -> bool:
        pass

    @abstractmethod
    def mark_candle_processed(
        self, event_id: str, recommendation: Recommendation
    ) -> None:
        pass

    @abstractmethod
    def get_ticker_state(self, run_id: str, ticker: str) -> TickerState:
        pass

    @abstractmethod
    def update_position(self, ticker: str, position_quantity: int) -> None:
        pass
