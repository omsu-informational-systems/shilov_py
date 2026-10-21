import uuid

from .entities import CandleEvent, RecommendationDecision, Signal


def evaluate_candle(candle: CandleEvent) -> RecommendationDecision:
    return RecommendationDecision(
        recommendation_id=str(uuid.uuid4()),
        source_event_id=candle.event_id,
        correlation_id=candle.correlation_id,
        run_id=candle.run_id,
        instrument=candle.ticker,
        sequence_number=candle.sequence_number,
        signal=Signal.HOLD,
        confidence=0,
        suggested_quantity=0,
        data_quality="OK",
        reason="Учебная заглушка",
        processing_time_ms=0,
    )
