import time
from dataclasses import replace

from ..core.entities import Candle
from ..core.logic import calculate_momentum
from ..ports.interfaces import RecommendationPublisherPort, StrategyStateRepositoryPort


class ProcessCandleUseCase:
    def __init__(
        self,
        publisher: RecommendationPublisherPort,
        state_repo: StrategyStateRepositoryPort,
        window_size: int = 5,
    ):
        self.publisher = publisher
        self.state_repo = state_repo
        self.window_size = window_size

        self.processed_candles: int = 0
        self.published_recommendations: int = 0
        self.last_sequence_number: int | None = None

    def execute(self, candle: Candle) -> bool:
        # Идемпотентность по eventId свечи (Глава 10)
        if self.state_repo.is_candle_processed(candle.event_id):
            return False

        start_ns = time.perf_counter_ns()
        ticker_state = self.state_repo.get_ticker_state(candle.run_id, candle.ticker)

        recommendation = calculate_momentum(
            candle=candle,
            state=ticker_state,
            window_size=self.window_size,
        )

        elapsed_ms = max(0, (time.perf_counter_ns() - start_ns) // 1_000_000)
        recommendation = replace(recommendation, processing_time_ms=int(elapsed_ms))

        # Публикуем в Kafka и фиксируем состояние
        self.publisher.publish_recommendation(recommendation)
        self.state_repo.mark_candle_processed(candle.event_id, recommendation)

        self.processed_candles += 1
        self.published_recommendations += 1
        self.last_sequence_number = candle.sequence_number
        return True

    def handle_execution_result(self, ticker: str, position_quantity: int) -> None:
        self.state_repo.update_position(ticker, position_quantity)
