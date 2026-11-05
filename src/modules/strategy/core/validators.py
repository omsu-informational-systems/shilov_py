from decimal import Decimal, InvalidOperation
from typing import Any

from .entities import CandleClosedPayload, InstrumentInfo


class MessageValidationError(Exception):
    """Исключение при нарушении схемы или отсутствии обязательных полей."""


def validate_envelope_structure(raw_event: Any) -> None:
    """Проверяет корневую структуру конверта."""
    if not isinstance(raw_event, dict):
        raise MessageValidationError("Сообщение не является JSON-объектом")

    required_envelope_fields = [
        "eventId",
        "eventType",
        "schemaVersion",
        "occurredAt",
        "correlationId",
        "producer",
        "payload",
    ]
    for field in required_envelope_fields:
        if field not in raw_event:
            raise MessageValidationError(
                f"Отсутствует обязательное поле конверта: {field}"
            )

    if not isinstance(raw_event["payload"], dict):
        raise MessageValidationError("Поле payload должно быть JSON-объектом")

    # if str(raw_event.get("schemaVersion")) != "1.0":
    #     raise MessageValidationError(
    #         f"Неподдерживаемая версия схемы: {raw_event.get('schemaVersion')}"
    #     )


def validate_candle_payload(payload: dict[str, Any]) -> CandleClosedPayload:
    """
    Проверяет обязательные поля события market.candle.closed
    Возвращает строго типизированный объект CandleClosedPayload.
    """
    required_fields = [
        "runId",
        "instrument",
        "sequenceNumber",
        "open",
        "high",
        "low",
        "close",
    ]
    for field in required_fields:
        if field not in payload or payload[field] is None:
            raise MessageValidationError(
                f"В payload свечи отсутствует обязательное поле: {field}"
            )

    instrument_raw = payload.get("instrument")
    if not isinstance(instrument_raw, dict) or not instrument_raw.get("ticker"):
        raise MessageValidationError("Поле instrument должно содержать непустой ticker")

    price_fields = ["open", "high", "low", "close"]
    for price_field in price_fields:
        try:
            val = Decimal(str(payload[price_field]))
            if val <= 0:
                raise MessageValidationError(
                    f"Цена {price_field} должна быть положительной: {val}"
                )
        except (InvalidOperation, TypeError):
            raise MessageValidationError(
                f"Некорректный десятичный формат цены в поле {price_field}: {payload[price_field]}"
            )

    try:
        seq = int(payload["sequenceNumber"])
    except (ValueError, TypeError):
        raise MessageValidationError(
            f"sequenceNumber должен быть целым числом: {payload['sequenceNumber']}"
        )

    instrument = InstrumentInfo(
        ticker=str(instrument_raw["ticker"]),
        figi=str(instrument_raw.get("figi", "")),
        currency=str(instrument_raw.get("currency", "RUB")),
        lotSize=int(instrument_raw.get("lotSize", 1)),
    )

    return CandleClosedPayload(
        runId=str(payload["runId"]),
        groupId=str(payload.get("groupId", "market")),
        instrument=instrument,
        timeframe=str(payload.get("timeframe", "1m")),
        sequenceNumber=seq,
        candleTime=str(payload.get("candleTime", "")),
        tradingDate=str(payload.get("tradingDate", "")),
        sessionPhase=str(payload.get("sessionPhase", "MAIN_SESSION")),
        open=str(payload["open"]),
        high=str(payload["high"]),
        low=str(payload["low"]),
        close=str(payload["close"]),
        volume=int(payload.get("volume", 0)),
        isWarmup=bool(payload.get("isWarmup", False)),
    )
