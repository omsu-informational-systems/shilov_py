import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.config import AppConfig
from src.modules.strategy.adapters.kafka_worker import KafkaAdapter
from src.modules.strategy.adapters.state_store import InMemoryStateRepository
from src.modules.strategy.application.use_cases import ProcessCandleUseCase

config = AppConfig.from_env()
state_repo = InMemoryStateRepository()
kafka_adapter = KafkaAdapter(config)
process_candle_uc = ProcessCandleUseCase(
    publisher=kafka_adapter,
    state_repo=state_repo,
    window_size=5,
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
    kafka_adapter.stop_event.set()
    if worker_thread and worker_thread.is_alive():
        worker_thread.join(timeout=10)


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check():
    broker_up = kafka_adapter.status == "UP"
    broker_comp = {"status": "UP" if broker_up else "DOWN"}
    if kafka_adapter.error_details:
        broker_comp["details"] = kafka_adapter.error_details

    return {
        "status": "UP" if broker_up else "DEGRADED",
        "service": config.service_name,
        "version": config.app_version,
        "components": {
            "messageBroker": broker_comp,
        },
    }
