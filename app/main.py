import asyncio
import structlog
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.api.routes import router
from app.workers.kafka_worker import KafkaWorker
from app.config import settings

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

kafka_worker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_worker
    logger.info("Starting Compute Service")
    try:
        from app.models.database import engine
        from app.models.receipt import Base
        
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error("Error creating database tables", error=str(e))

    kafka_worker = KafkaWorker()
    try:
        asyncio.create_task(kafka_worker.start())
        logger.info("Kafka worker started in background")
    except Exception as e:
        logger.error("Failed to start Kafka worker", error=str(e))
    
    yield

    logger.info("Stopping Compute Service")
    
    if kafka_worker:
        try:
            await kafka_worker.stop()
            logger.info("Kafka worker stopped")
        except Exception as e:
            logger.error("Error stopping Kafka worker", error=str(e))


app = FastAPI(
    title="Compute Service",
    description="Микросервис для кластерных вычислений по чекам",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
async def root():
    return {"message": "Spark servicee"}


@app.get("/status")
async def get_status():
    global kafka_worker
    
    status = {
        "service": "Compute Service",
        "status": "running",
        "kafka_worker": "stopped"
    }
    
    if kafka_worker:
        try:
            worker_status = await kafka_worker.get_status()
            status["kafka_worker"] = "running" if worker_status["is_running"] else "stopped"
            status["kafka_details"] = worker_status
        except Exception as e:
            logger.error("Error getting worker status", error=str(e))
            status["kafka_worker"] = "error"
    
    return status


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
        log_level=settings.log_level.lower()
    )
