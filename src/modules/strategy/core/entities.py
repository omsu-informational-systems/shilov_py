import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utc_now() -> str:
    """Возвращает текущее время UTC в формате ISO 8601 с суффиксом Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class SignalType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class DataQuality(str, Enum):
    OK = "OK"
    DEGRADED = "DEGRADED"


class RegistrationStatus(str, Enum):
    REGISTERED = "REGISTERED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class MessageEnvelope:
    """
    Единый JSON-конверт платформы для всех типов событий.
    Присутствует во всех топиках без исключений.
    """

    eventType: str
    producer: str
    payload: dict[str, Any]
    schemaVersion: str = "1.0"
    eventId: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurredAt: str = field(default_factory=utc_now)
    correlationId: str | None = None
    causationId: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Преобразует конверт в словарь для сериализации в JSON."""
        data = asdict(self)
        if data["correlationId"] is None:
            data["correlationId"] = self.eventId
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MessageEnvelope":
        return cls(
            eventId=data["eventId"],
            eventType=data["eventType"],
            schemaVersion=data.get("schemaVersion", "1.0"),
            occurredAt=data["occurredAt"],
            correlationId=data.get("correlationId"),
            causationId=data.get("causationId"),
            producer=data["producer"],
            payload=data["payload"],
        )


@dataclass(frozen=True)
class RegistrationRequestPayload:
    """Полезная нагрузка запроса регистрации (account.registration.request)."""

    secretKey: str
    accountId: str
    accountName: str
    groupId: str
    strategyId: str
    strategyName: str
    serviceName: str
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RegistrationResultPayload:
    """Полезная нагрузка ответа платформы (account.registration.result)."""

    secretKey: str
    accountId: str
    groupId: str
    strategyId: str
    serviceName: str
    status: str
    reason: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RegistrationResultPayload":
        return cls(
            secretKey=data["secretKey"],
            accountId=data["accountId"],
            groupId=data["groupId"],
            strategyId=data["strategyId"],
            serviceName=data["serviceName"],
            status=data["status"],
            reason=data.get("reason", ""),
        )


@dataclass(frozen=True)
class HeartbeatPayload:
    """Полезная нагрузка сигнала состояния (account.service.heartbeat)."""

    secretKey: str
    accountId: str
    groupId: str
    strategyId: str
    serviceName: str
    instanceId: str
    status: str
    startedAt: str
    sentAt: str
    processedCandles: int
    publishedRecommendations: int
    lastProcessedSequenceNumber: int | None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InstrumentInfo:
    ticker: str
    figi: str = ""
    currency: str = "RUB"
    lotSize: int = 1


@dataclass(frozen=True)
class CandleClosedPayload:
    """Полезная нагрузка закрытой свечи (market.candle.closed)."""

    runId: str
    groupId: str
    instrument: InstrumentInfo
    timeframe: str
    sequenceNumber: int
    candleTime: str
    tradingDate: str
    sessionPhase: str
    open: str
    high: str
    low: str
    close: str
    volume: int
    isWarmup: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CandleClosedPayload":
        inst_data = data.get("instrument", {})
        instrument = InstrumentInfo(
            ticker=inst_data.get("ticker", ""),
            figi=inst_data.get("figi", ""),
            currency=inst_data.get("currency", "RUB"),
            lotSize=int(inst_data.get("lotSize", 1)),
        )
        return cls(
            runId=data["runId"],
            groupId=data.get("groupId", "market"),
            instrument=instrument,
            timeframe=data.get("timeframe", "1m"),
            sequenceNumber=int(data["sequenceNumber"]),
            candleTime=data.get("candleTime", ""),
            tradingDate=data.get("tradingDate", ""),
            sessionPhase=data.get("sessionPhase", "MAIN_SESSION"),
            open=str(data["open"]),
            high=str(data["high"]),
            low=str(data["low"]),
            close=str(data["close"]),
            volume=int(data.get("volume", 0)),
            isWarmup=bool(data.get("isWarmup", False)),
        )


@dataclass(frozen=True)
class RecommendationCreatedPayload:
    """Полезная нагрузка торгового решения (strategy.recommendation.created)."""

    recommendationId: str
    sourceEventId: str
    runId: str
    secretKey: str
    strategyId: str
    accountId: str
    groupId: str
    instrument: str
    sequenceNumber: int
    signal: str
    confidence: float
    suggestedQuantity: int
    dataQuality: str
    reason: str
    processingTimeMs: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
