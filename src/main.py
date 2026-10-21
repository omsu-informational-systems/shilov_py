import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import AppConfig
from src.modules.strategy.adapters.kafka_worker import (
    InMemoryDeduplicationStore,
    KafkaAdapter,
)
from src.modules.strategy.application.process_candle import ProcessCandleUseCase

config = AppConfig.from_env()
kafka_adapter = KafkaAdapter(config)
dedup_store = InMemoryDeduplicationStore()
process_candle_uc = ProcessCandleUseCase(
    publisher=kafka_adapter, dedup_store=dedup_store
)

worker_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_thread
    worker_thread = threading.Thread(
        target=kafka_adapter.run_loop,
        args=(process_candle_uc,),
        daemon=True,
    )
    worker_thread.start()

    yield

    # Корректное завершение: сигнализируем циклу остановиться и ждем закрытия консьюмеров
    kafka_adapter.stop_event.set()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    is_up = kafka_adapter.status == "UP"
    kafka_component = {"status": "UP" if is_up else "DOWN"}
    if kafka_adapter.error_details:
        kafka_component["details"] = kafka_adapter.error_details

    return {
        "status": "UP" if is_up else "DEGRADED",
        "service": config.service_name,
        "version": config.app_version,
        "components": {
            "kafka": kafka_component,
        },
    }
