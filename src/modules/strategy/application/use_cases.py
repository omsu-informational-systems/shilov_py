import logging

from ..core.entities import CandleClosedEvent
from ..ports.interfaces import CandleHandlerPort

logger = logging.getLogger("strategy.application")


class ReceiveCandleUseCase(CandleHandlerPort):
    def __init__(self) -> None:
        self.processed_candles: int = 0
        self.last_sequence_number: int | None = None

    def handle_candle(self, candle: CandleClosedEvent) -> None:
        self.processed_candles += 1
        self.last_sequence_number = candle.sequence_number

        logger.info(
            "event=candle_received eventId=%s correlationId=%s runId=%s "
            "ticker=%s timeframe=%s seq=%s close=%s volume=%s",
            candle.event_id,
            candle.correlation_id,
            candle.run_id,
            candle.ticker,
            candle.timeframe,
            candle.sequence_number,
            candle.close_price,
            candle.volume,
        )
