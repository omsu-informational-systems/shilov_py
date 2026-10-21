from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any


class Signal(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class DataQuality(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True)
class Candle:
    event_id: str
    correlation_id: str
    run_id: str
    ticker: str
    lot_size: int
    timeframe: str
    sequence_number: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    session_phase: str = "MAIN_SESSION"
    is_warmup: bool = False


@dataclass
class TickerState:
    last_sequence_number: int | None = None
    close_prices: list[Decimal] = field(default_factory=list)
    position_quantity: int = 0  # В бумагах (из strategy.recommendation.result)


@dataclass(frozen=True)
class Recommendation:
    recommendation_id: str
    source_event_id: str
    correlation_id: str
    run_id: str
    instrument: str
    sequence_number: int
    signal: Signal
    confidence: float
    suggested_quantity: int
    data_quality: DataQuality
    reason: str
    processing_time_ms: int
    metadata: dict[str, Any] = field(default_factory=dict)
