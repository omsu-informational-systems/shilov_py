from ..core.entities import CandleEvent
from ..core.logic import evaluate_candle
from ..ports.interfaces import DeduplicationStorePort, RecommendationPublisherPort


class ProcessCandleUseCase:
    def __init__(
        self,
        publisher: RecommendationPublisherPort,
        dedup_store: DeduplicationStorePort,
    ):
        self.publisher = publisher
        self.dedup_store = dedup_store
        self.processed_candles: int = 0
        self.published_recommendations: int = 0
        self.last_sequence_number: int | None = None

    def execute(self, candle: CandleEvent) -> bool:
        if self.dedup_store.is_seen(candle.event_id):
            return False

        decision = evaluate_candle(candle)
        self.publisher.publish_recommendation(decision)

        self.dedup_store.mark_seen(candle.event_id)
        self.processed_candles += 1
        self.published_recommendations += 1
        self.last_sequence_number = candle.sequence_number
        return True
