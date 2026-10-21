from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Signal(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass(frozen=True)
class CandleEvent:
    event_id: str
    correlation_id: str
    run_id: str
    ticker: str
    sequence_number: int
    close_price: str  # цены передаются строками ("281.40")


@dataclass(frozen=True)
class RecommendationDecision:
    recommendation_id: str
    source_event_id: str
    correlation_id: str
    run_id: str
    instrument: str
    sequence_number: int
    signal: Signal
    confidence: float = 0.0
    suggested_quantity: int = 0
    data_quality: str = "OK"
    reason: str = "Учебная заглушка"
    processing_time_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
