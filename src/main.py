import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

load_dotenv()
SERVICE_NAME = os.getenv("SERVICE_NAME")
VERSION = os.getenv("APP_VERSION", "1.0.0")


@app.get("/health")
async def health_check():
    return JSONResponse(
        status_code=200,
        content={
            "status": "DEGRADED",
            "service": SERVICE_NAME,
            "version": VERSION,
            "components": {
                "database": {"status": "DOWN", "details": "connection unavailable"}
            },
        },
    )
