import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class MessageEnvelope:
    """Общая структура любого сообщения платформы."""

    eventType: str
    producer: str
    payload: dict[str, Any]
    schemaVersion: str = "1.0"
    eventId: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurredAt: str = field(default_factory=utc_now)
    correlationId: str | None = None
    causationId: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if data["correlationId"] is None:
            data["correlationId"] = self.eventId
        return data


@dataclass(frozen=True)
class RegistrationRequestPayload:
    """Модель запроса регистрации."""

    secretKey: str
    accountId: str
    accountName: str
    groupId: str
    strategyId: str
    strategyName: str
    serviceName: str
    description: str


@dataclass(frozen=True)
class CandleClosedEvent:
    """Доменная модель полученной свечи."""

    event_id: str
    correlation_id: str
    run_id: str
    ticker: str
    timeframe: str
    sequence_number: int
    candle_time: str
    open_price: str
    high_price: str
    low_price: str
    close_price: str
    volume: int
