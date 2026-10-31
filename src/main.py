import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import AppConfig
from src.modules.strategy.adapters.kafka_consumer import KafkaConsumerAdapter
from src.modules.strategy.adapters.kafka_producer import KafkaProducerAdapter
from src.modules.strategy.application.receive_candle import ReceiveCandleUseCase
from src.modules.strategy.application.register_service import RegisterServiceUseCase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

config = AppConfig.load()

producer_adapter = KafkaProducerAdapter(config)
register_use_case = RegisterServiceUseCase(publisher=producer_adapter, config=config)
candle_use_case = ReceiveCandleUseCase()

consumer_adapter = KafkaConsumerAdapter(
    config=config,
    register_use_case=register_use_case,
    candle_handler=candle_use_case,
)

worker_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_thread
    logger.info("Запуск фонового потока Kafka...")
    worker_thread = threading.Thread(target=consumer_adapter.run_loop, daemon=True)
    worker_thread.start()

    yield

    logger.info("Остановка сервиса...")
    consumer_adapter.stop_event.set()
    producer_adapter.close()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=5)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    is_up = consumer_adapter.status == "UP"
    broker_info = {"status": "UP" if is_up else "DOWN"}
    if consumer_adapter.error_details:
        broker_info["details"] = consumer_adapter.error_details

    return {
        "status": "UP" if is_up else "DEGRADED",
        "service": config.service_name,
        "version": config.app_version,
        "components": {
            "messageBroker": broker_info,
        },
    }
