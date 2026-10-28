import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import AppConfig
from src.modules.strategy.adapters.kafka_client import KafkaWorkerAdapter
from src.modules.strategy.application.use_cases import ReceiveCandleUseCase

config = AppConfig.from_env()
candle_use_case = ReceiveCandleUseCase()
kafka_worker = KafkaWorkerAdapter(config=config, candle_handler=candle_use_case)

worker_thread: threading.Thread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_thread
    worker_thread = threading.Thread(target=kafka_worker.run, daemon=True)
    worker_thread.start()
    yield
    kafka_worker.stop_event.set()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    broker_up = kafka_worker.status == "UP"
    broker_info = {"status": "UP" if broker_up else "DOWN"}
    if kafka_worker.error_details:
        broker_info["details"] = kafka_worker.error_details

    return {
        "status": "UP" if broker_up else "DEGRADED",
        "service": config.service_name,
        "version": config.app_version,
        "components": {
            "messageBroker": broker_info,
        },
    }
