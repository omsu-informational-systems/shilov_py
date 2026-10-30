import logging

from ..core.entities import CandleClosedPayload
from ..ports.interfaces import CandleHandlerPort

logger = logging.getLogger("strategy.candles")


class ReceiveCandleUseCase(CandleHandlerPort):
    def __init__(self) -> None:
        self.processed_candles: int = 0
        self.last_sequence_number: int | None = None

    def handle_candle(
        self, event_id: str, correlation_id: str, candle: CandleClosedPayload
    ) -> None:
        self.processed_candles += 1
        self.last_sequence_number = candle.sequenceNumber

        # Структурированный лог в формате key=value
        logger.info(
            "event=candle_received eventId=%s correlationId=%s runId=%s "
            "ticker=%s seq=%s close=%s volume=%s sessionPhase=%s",
            event_id,
            correlation_id,
            candle.runId,
            candle.instrument.ticker,
            candle.sequenceNumber,
            candle.close,
            candle.volume,
            candle.sessionPhase,
        )
