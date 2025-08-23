import json
import asyncio
import structlog
from aiokafka import AIOKafkaConsumer
from app.config import settings
from app.schemas.receipt import ReceiptCreate

logger = structlog.get_logger()


class KafkaService:
    def __init__(self):
        self.consumer = None
        self.is_running = False
        
    async def start(self):
        self.consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        await self.consumer.start()
        self.is_running = True
        logger.info("Kafka consumer started", topic=settings.kafka_topic)
        
    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            self.is_running = False
            logger.info("Kafka consumer stopped")
            
    async def consume_messages(self, callback):
        try:
            async for message in self.consumer:
                try:
                    receipt_data = message.value
                    receipt = ReceiptCreate(**receipt_data)
                    await callback(receipt)
                    logger.info("Message processed", 
                              receipt_id=receipt.id, 
                              partition=message.partition,
                              offset=message.offset)
                except Exception as e:
                    logger.error("Error processing message", 
                               error=str(e), 
                               data=message.value)
        except Exception as e:
            logger.error("Kafka consumer error", error=str(e))
            
    async def get_message_count(self):
        if not self.consumer:
            return 0
        
        partitions = self.consumer.assignment()
        if not partitions:
            return 0
            
        end_offsets = await self.consumer.end_offsets(partitions)
        total_messages = sum(end_offsets.values())
        return total_messages
