import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Не задана обязательная переменная окружения: {name}")
    return value


@dataclass(frozen=True)
class AppConfig:
    bootstrap_servers: str
    kafka_username: str
    kafka_password: str
    secret_key: str
    account_id: str
    account_name: str
    group_id: str
    strategy_id: str
    service_name: str
    kafka_group_id: str
    candle_topic: str
    reg_request_topic: str
    reg_result_topic: str
    heartbeat_topic: str
    app_version: str

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "omsu.7bits.it:9094"
            ),
            kafka_username=required("KAFKA_USERNAME"),
            kafka_password=required("KAFKA_PASSWORD"),
            secret_key=required("ACCOUNT_SECRET_KEY"),
            account_id=required("ACCOUNT_ID"),
            account_name=required("ACCOUNT_NAME"),
            group_id=required("GROUP_ID"),
            strategy_id=required("STRATEGY_ID"),
            service_name=required("SERVICE_NAME"),
            kafka_group_id=required("KAFKA_GROUP_ID"),
            candle_topic=required("CANDLE_TOPIC"),
            reg_request_topic=os.environ.get(
                "REG_REQUEST_TOPIC", "account.registration.request"
            ),
            reg_result_topic=os.environ.get(
                "REG_RESULT_TOPIC", "account.registration.result"
            ),
            heartbeat_topic=os.environ.get(
                "HEARTBEAT_TOPIC", "account.service.heartbeat"
            ),
            app_version=os.environ.get("APP_VERSION", "1.0.0"),
        )
