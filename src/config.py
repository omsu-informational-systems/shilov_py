import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _get_required(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise ValueError(f"Отсутствует обязательная переменная окружения: {key}")
    return val


@dataclass(frozen=True)
class AppConfig:
    # Сеть и авторизация
    bootstrap_servers: str
    kafka_username: str
    kafka_password: str
    kafka_ca_location: str | None

    # Идентификаторы аккаунта и сервиса
    secret_key: str
    account_id: str
    account_name: str
    group_id: str
    strategy_id: str
    service_name: str
    app_version: str

    # Технический group.id Kafka
    kafka_group_id: str

    # Названия топиков
    candle_topic: str
    reg_request_topic: str
    reg_result_topic: str
    heartbeat_topic: str
    rec_created_topic: str
    rec_result_topic: str

    @classmethod
    def load(cls) -> "AppConfig":
        return cls(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "omsu.7bits.it:9094"
            ),
            kafka_username=_get_required("KAFKA_USERNAME"),
            kafka_password=_get_required("KAFKA_PASSWORD"),
            kafka_ca_location=os.environ.get("KAFKA_CA_LOCATION"),
            secret_key=_get_required("ACCOUNT_SECRET_KEY"),
            account_id=_get_required("ACCOUNT_ID"),
            account_name=_get_required("ACCOUNT_NAME"),
            group_id=_get_required("GROUP_ID"),
            strategy_id=_get_required("STRATEGY_ID"),
            service_name=_get_required("SERVICE_NAME"),
            app_version=os.environ.get("APP_VERSION", "1.0.0"),
            kafka_group_id=_get_required("KAFKA_GROUP_ID"),
            candle_topic=_get_required("CANDLE_TOPIC"),
            reg_request_topic=os.environ.get(
                "REG_REQUEST_TOPIC", "account.registration.request"
            ),
            reg_result_topic=os.environ.get(
                "REG_RESULT_TOPIC", "account.registration.result"
            ),
            heartbeat_topic=os.environ.get(
                "HEARTBEAT_TOPIC", "account.service.heartbeat"
            ),
            rec_created_topic=os.environ.get(
                "REC_CREATED_TOPIC", "strategy.recommendation.created"
            ),
            rec_result_topic=os.environ.get(
                "REC_RESULT_TOPIC", "strategy.recommendation.result"
            ),
        )
