import structlog
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from typing import Dict, List, Any
from app.config import settings

logger = structlog.get_logger()


class SparkService:
    def __init__(self):
        self.spark = None
        self.is_initialized = False
        
    def initialize_spark(self):
        try:
            self.spark = SparkSession.builder \
                .appName(settings.spark_app_name) \
                .master(settings.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .getOrCreate()
                
            self.is_initialized = True
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error("Error initializing Spark", error=str(e))
            raise
            
    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            self.is_initialized = False
            logger.info("Spark session stopped")
            
    def load_data_from_postgres(self, table_name: str):
        if not self.is_initialized:
            self.initialize_spark()
            
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", settings.database_url.replace("+asyncpg", "")) \
                .option("dbtable", table_name) \
                .option("user", "user") \
                .option("password", "password") \
                .load()
                
            logger.info(f"Data loaded from {table_name}", count=df.count())
            return df
            
        except Exception as e:
            logger.error(f"Error loading data from {table_name}", error=str(e))
            return None
            
    def calculate_monthly_receipts(self) -> Dict[str, Any]:
        try:
            checks_df = self.load_data_from_postgres("checks")
            if checks_df is None:
                return {}
                
            monthly_stats = checks_df \
                .withColumn("month", date_format(col("created_at"), "yyyy-MM")) \
                .groupBy("month") \
                .agg(
                    count("check_id").alias("receipts_count"),
                    sum("check_sum").alias("total_revenue"),
                    avg("check_sum").alias("avg_receipt")
                ) \
                .orderBy("month")
                
            result = monthly_stats.toPandas().to_dict('records')
            logger.info("Monthly receipts calculated", count=len(result))
            return {"monthly_receipts": result}
            
        except Exception as e:
            logger.error("Error calculating monthly receipts", error=str(e))
            return {}
            
    def calculate_payment_types(self) -> Dict[str, Any]:
        try:
            invoices_df = self.load_data_from_postgres("invoices")
            if invoices_df is None:
                return {}
                
            payment_stats = invoices_df \
                .filter(col("payment_type").isNotNull()) \
                .groupBy("payment_type") \
                .agg(
                    count("*").alias("count"),
                    sum("invoice_sum").alias("total_sum")
                )
                
            result = payment_stats.toPandas().to_dict('records')
            logger.info("Payment types calculated", count=len(result))
            return {"payment_types": result}
            
        except Exception as e:
            logger.error("Error calculating payment types", error=str(e))
            return {}
            
    def calculate_product_analytics(self) -> Dict[str, Any]:
        try:
            items_df = self.load_data_from_postgres("items")
            if items_df is None:
                return {}

            top_products = items_df \
                .groupBy("item_name") \
                .agg(
                    sum("item_sum").alias("total_sales"),
                    count("*").alias("sales_count"),
                    avg("item_price").alias("avg_price")
                ) \
                .orderBy(col("total_sales").desc()) \
                .limit(10)

            product_types = items_df \
                .groupBy("item_type") \
                .agg(
                    count("*").alias("count"),
                    sum("item_sum").alias("total_sales"),
                    avg("item_price").alias("avg_price")
                )
                
            result = {
                "top_products": top_products.toPandas().to_dict('records'),
                "product_types": product_types.toPandas().to_dict('records')
            }
            
            logger.info("Product analytics calculated")
            return result
            
        except Exception as e:
            logger.error("Error calculating product analytics", error=str(e))
            return {}
            
    def calculate_daily_stats(self) -> Dict[str, Any]:
        try:
            checks_df = self.load_data_from_postgres("checks")
            if checks_df is None:
                return {}
                
            daily_stats = checks_df \
                .withColumn("date", date_format(col("created_at"), "yyyy-MM-dd")) \
                .groupBy("date") \
                .agg(
                    count("check_id").alias("receipts_count"),
                    sum("check_sum").alias("daily_revenue"),
                    avg("check_sum").alias("avg_receipt"),
                    countDistinct("user_id").alias("unique_customers")
                ) \
                .orderBy("date")
                
            result = daily_stats.toPandas().to_dict('records')
            logger.info("Daily stats calculated", count=len(result))
            return {"daily_stats": result}
            
        except Exception as e:
            logger.error("Error calculating daily stats", error=str(e))
            return {}
            
    def run_comprehensive_analytics(self) -> Dict[str, Any]:
        try:
            logger.info("Starting comprehensive analytics")
            
            results = {}
            results.update(self.calculate_monthly_receipts())
            results.update(self.calculate_payment_types())
            results.update(self.calculate_product_analytics())
            results.update(self.calculate_daily_stats())
            
            logger.info("Comprehensive analytics completed")
            return results
            
        except Exception as e:
            logger.error("Error in comprehensive analytics", error=str(e))
            return {}
