import asyncio
import structlog
from app.services.kafka_service import KafkaService
from app.services.analytics_service import AnalyticsService
from app.schemas.receipt import ReceiptCreate

logger = structlog.get_logger()


class KafkaWorker:
    def __init__(self):
        self.kafka_service = KafkaService()
        self.analytics_service = AnalyticsService()
        self.is_running = False
        
    async def start(self):
        try:
            await self.kafka_service.start()
            self.is_running = True
            logger.info("Kafka worker started")

            await self.kafka_service.consume_messages(self._process_message)
            
        except Exception as e:
            logger.error("Error starting Kafka worker", error=str(e))
            raise
            
    async def stop(self):
        try:
            self.is_running = False
            await self.kafka_service.stop()
            await self.analytics_service.cleanup()
            logger.info("Kafka worker stopped")
        except Exception as e:
            logger.error("Error stopping Kafka worker", error=str(e))
            
    async def _process_message(self, receipt_data: ReceiptCreate):
        try:
            success = await self.analytics_service.process_receipt(receipt_data)
            
            if success:
                logger.info("Message processed successfully", receipt_id=receipt_data.id)
            else:
                logger.error("Failed to process message", receipt_id=receipt_data.id)
                
        except Exception as e:
            logger.error("Error processing message", error=str(e), receipt_id=receipt_data.id)
            
    async def get_status(self):
        return {
            "is_running": self.is_running,
            "kafka_connected": self.kafka_service.is_running,
            "message_count": await self.kafka_service.get_message_count()
        }
