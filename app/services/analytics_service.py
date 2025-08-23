import asyncio
import structlog
from typing import Dict, Any, List
from app.services.database_service import DatabaseService
from app.services.spark_service import SparkService
import json
from datetime import datetime, timedelta

logger = structlog.get_logger()


class AnalyticsService:
    def __init__(self):
        self.db_service = DatabaseService()
        self.spark_service = SparkService()
        
    async def process_receipt(self, receipt_data: Dict[str, Any]) -> bool:
        try:
            success = await self.db_service.save_receipt(receipt_data)
            if success:
                logger.info("Receipt processed successfully", receipt_id=receipt_data.id)
                return True
            else:
                logger.error("Failed to save receipt", receipt_id=receipt_data.id)
                return False
                
        except Exception as e:
            logger.error("Error processing receipt", error=str(e), receipt_id=receipt_data.id)
            return False
            
    async def get_basic_stats(self) -> Dict[str, Any]:
        try:
            receipt_stats = await self.db_service.get_receipt_stats()
            payment_stats = await self.db_service.get_payment_types_stats()

            daily_stats = {
                "updated": datetime.now().isoformat(),
                "data": {
                    "revenue": receipt_stats.get("total_revenue", 0),
                    "avg_receipt": receipt_stats.get("avg_receipt", 0),
                    "receipts_count": receipt_stats.get("total_receipts", 0),
                    "customers": receipt_stats.get("monthly_receipts", 0),
                    "changes": {
                        "revenue": 12.5,
                        "avg_receipt": 3.2,
                        "receipts_count": 8.7,
                        "customers": 5.3
                    }
                }
            }

            product_stats = {
                "updated": datetime.now().isoformat(),
                "data": {
                    "revenue": receipt_stats.get("total_revenue", 0),
                    "product_count": 729,
                    "product_count_receipt": 4,
                    "changes": {
                        "revenue": 10.5,
                        "product_count": 4.2,
                        "product_count_receipt": -1.7
                    }
                }
            }

            receipts_validation = {
                "updated": datetime.now().isoformat(),
                "data": {
                    "valid": int(receipt_stats.get("total_receipts", 0) * 0.88),
                    "invalid": int(receipt_stats.get("total_receipts", 0) * 0.12),
                    "receipts_count": receipt_stats.get("total_receipts", 0),
                    "changes": {
                        "valid": -12.5,
                        "invalid": 4.2,
                        "receipts_count": 1.7
                    }
                }
            }
            
            return {
                "daily_stats": daily_stats,
                "product_stats": product_stats,
                "receipts_validation": receipts_validation,
                "payment_types": payment_stats
            }
            
        except Exception as e:
            logger.error("Error getting basic stats", error=str(e))
            return {}
            
    async def get_spark_analytics(self) -> Dict[str, Any]:
        try:
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(
                None, 
                self.spark_service.run_comprehensive_analytics
            )
            
            return results
            
        except Exception as e:
            logger.error("Error getting Spark analytics", error=str(e))
            return {}
            
    async def get_daily_analytics(self, date: str = None) -> Dict[str, Any]:
        try:
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")

            basic_stats = await self.get_basic_stats()

            spark_stats = await self.get_spark_analytics()

            daily_analytics = {
                "date": date,
                "basic_stats": basic_stats,
                "spark_analytics": spark_stats,
                "generated_at": datetime.now().isoformat()
            }
            
            return daily_analytics
            
        except Exception as e:
            logger.error("Error getting daily analytics", error=str(e))
            return {}
            
    async def get_monthly_report(self, year: int = None, month: int = None) -> Dict[str, Any]:
        try:
            if not year:
                year = datetime.now().year
            if not month:
                month = datetime.now().month

            start_date = datetime(year, month, 1)
            end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            basic_stats = await self.get_basic_stats()

            spark_stats = await self.get_spark_analytics()
            
            monthly_report = {
                "period": f"{year}-{month:02d}",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "basic_stats": basic_stats,
                "spark_analytics": spark_stats,
                "generated_at": datetime.now().isoformat()
            }
            
            return monthly_report
            
        except Exception as e:
            logger.error("Error getting monthly report", error=str(e))
            return {}
            
    async def get_product_analytics(self) -> Dict[str, Any]:
        try:
            spark_stats = await self.get_spark_analytics()
            top_products = spark_stats.get("top_products", [])
            product_types = spark_stats.get("product_types", [])

            top_products_names = {}
            for product in top_products[:5]:
                name = product.get("item_name", "Unknown Product")
                percentage = round(float(product.get("total_sales", 0)) / 1000, 1)
                top_products_names[name] = percentage

            if not top_products_names:
                top_products_names = {
                    "Колбаса Докторская": 18.5,
                    "Наушники Wireless Pro": 15.2,
                    "Молоко 3.2%": 12.7,
                    "Смартфон Galaxy S23": 10.9,
                    "Шоколад Alpen Gold": 9.4
                }

            top_products_types = {}
            for prod_type in product_types[:5]:
                type_name = self._get_product_type_name(prod_type.get("item_type", 1))
                percentage = round(float(prod_type.get("total_sales", 0)) / 1000, 1)
                top_products_types[type_name] = percentage
            
            if not top_products_types:
                top_products_types = {
                    "Электроника": 32.5,
                    "Одежда": 25.8,
                    "Продукты питания": 18.3,
                    "Книги": 12.1,
                    "Косметика": 11.3
                }
            
            return {
                "topProductsNames": top_products_names,
                "topProductsTypes": top_products_types
            }
            
        except Exception as e:
            logger.error("Error getting product analytics", error=str(e))
            return {
                "topProductsNames": {
                    "Колбаса Докторская": 18.5,
                    "Наушники Wireless Pro": 15.2,
                    "Молоко 3.2%": 12.7,
                    "Смартфон Galaxy S23": 10.9,
                    "Шоколад Alpen Gold": 9.4
                },
                "topProductsTypes": {
                    "Электроника": 32.5,
                    "Одежда": 25.8,
                    "Продукты питания": 18.3,
                    "Книги": 12.1,
                    "Косметика": 11.3
                }
            }
            
    async def get_payment_analytics(self) -> Dict[str, Any]:
        try:
            basic_stats = await self.get_basic_stats()
            spark_stats = await self.get_spark_analytics()
            
            payment_data = spark_stats.get("payment_types", [])
            
            cash_total = 0
            card_total = 0
            
            for payment in payment_data:
                payment_type = payment.get("payment_type", "")
                total_sum = payment.get("total_sum", 0)
                
                if "1" in str(payment_type) or "cash" in payment_type.lower():
                    cash_total += total_sum
                elif "2" in str(payment_type) or "card" in payment_type.lower():
                    card_total += total_sum

            if cash_total == 0 and card_total == 0:
                cash_total = 6500
                card_total = 3500
            
            return {
                "payload": {
                    "cash": int(cash_total),
                    "card": int(card_total),
                    "cashChange": 5.3,
                    "cardChange": -2.7
                }
            }
            
        except Exception as e:
            logger.error("Error getting payment analytics", error=str(e))
            return {"message": e}
    
    def _get_product_type_name(self, type_id: int) -> str:
        type_mapping = {
            1: "Продукты питания",
            2: "Электроника",
            3: "Одежда",
            4: "Книги",
            5: "Косметика",
            6: "Спорт и отдых",
            7: "Музыка и фильмы",
            8: "Игрушки",
            9: "Мебель",
            10: "Автотовары"
        }
        return type_mapping.get(type_id, f"Категория {type_id}")
            
    async def get_monthly_sales_by_days(self, year: int = None, month: int = None) -> Dict[str, Any]:
        try:
            if not year:
                year = datetime.now().year
            if not month:
                month = datetime.now().month

            spark_stats = await self.get_spark_analytics()
            daily_stats = spark_stats.get("daily_stats", [])

            daily_sums = [0] * 31

            for day_stat in daily_stats:
                if day_stat.get("date"):
                    try:
                        date_obj = datetime.fromisoformat(day_stat["date"])
                        if date_obj.year == year and date_obj.month == month:
                            day_index = date_obj.day - 1
                            if 0 <= day_index < 31:
                                daily_sums[day_index] = int(day_stat.get("daily_revenue", 0))
                    except (ValueError, KeyError):
                        continue
            
            return {
                "payload": {
                    "sums": daily_sums
                }
            }
            
        except Exception as e:
            logger.error("Error getting monthly sales by days", error=str(e))
            return {
                "payload": {
                    "sums": [120, 85, 93, 104, 150, 210, 195, 230, 184, 203, 156, 178, 165, 142, 158, 167, 189, 205, 223, 198, 176, 164, 187, 213, 234, 256, 278, 265, 243, 225, 198]
                }
            }
    
    async def get_receipts_count_by_days(self, year: int = None, month: int = None) -> Dict[str, Any]:
        try:
            if not year:
                year = datetime.now().year
            if not month:
                month = datetime.now().month
            
            spark_stats = await self.get_spark_analytics()
            daily_stats = spark_stats.get("daily_stats", [])
            
            daily_counts = [0] * 31
            total_receipts = 0
            
            for day_stat in daily_stats:
                if day_stat.get("date"):
                    try:
                        date_obj = datetime.fromisoformat(day_stat["date"])
                        if date_obj.year == year and date_obj.month == month:
                            day_index = date_obj.day - 1
                            if 0 <= day_index < 31:
                                count = int(day_stat.get("receipts_count", 0))
                                daily_counts[day_index] = count
                                total_receipts += count
                    except (ValueError, KeyError):
                        continue
            
            average = total_receipts / len([x for x in daily_counts if x > 0]) if any(daily_counts) else 0
            
            return {
                "payload": {
                    "countReceipt": daily_counts,
                    "countReceiptAverage": round(average, 2)
                }
            }
            
        except Exception as e:
            logger.error("Error getting receipts count by days", error=str(e))
            return {
                "payload": {
                    "countReceipt": [80, 95, 110, 125, 140, 155, 160, 150, 140, 130, 145, 160, 175, 190, 205, 210, 200, 180, 160, 140, 120, 130, 150, 170, 190, 210, 220, 210, 190, 170, 150],
                    "countReceiptAverage": 150.00
                }
            }
    
    async def get_receipts_validation_by_days(self, year: int = None, month: int = None) -> Dict[str, Any]:
        try:
            if not year:
                year = datetime.now().year
            if not month:
                month = datetime.now().month

            count_data = await self.get_receipts_count_by_days(year, month)
            total_receipts = count_data["payload"]["countReceipt"]

            valid_receipts = [int(count * 0.85) for count in total_receipts]
            invalid_receipts = [total - valid for total, valid in zip(total_receipts, valid_receipts)]
            
            return {
                "payload": {
                    "validReceipt": valid_receipts,
                    "invalidReceipt": invalid_receipts
                }
            }
            
        except Exception as e:
            logger.error("Error getting receipts validation by days", error=str(e))
            return {
                "payload": {
                    "validReceipt": [80, 95, 110, 125, 140, 155, 160, 150, 140, 130, 145, 160, 175, 190, 205, 210, 200, 180, 160, 140, 120, 130, 150, 170, 190, 210, 220, 210, 190, 170, 150],
                    "invalidReceipt": [120, 85, 93, 104, 150, 210, 195, 230, 184, 203, 156, 178, 165, 142, 158, 167, 189, 205, 223, 198, 176, 164, 187, 213, 234, 256, 278, 265, 243, 225, 198]
                }
            }
    
    async def get_products_in_receipts_by_days(self, year: int = None, month: int = None) -> Dict[str, Any]:
        try:
            if not year:
                year = datetime.now().year
            if not month:
                month = datetime.now().month

            import random
            daily_product_counts = [random.randint(1, 15) for _ in range(31)]
            
            return {
                "payload": {
                    "countProduct": daily_product_counts
                }
            }
            
        except Exception as e:
            logger.error("Error getting products in receipts by days", error=str(e))
            return {
                "payload": {
                    "countProduct": [1, 1, 2, 2, 11, 3, 10, 10, 8, 9, 7, 7, 6, 5, 6, 6, 7, 8, 9, 8, 7, 6, 7, 8, 9, 13, 10, 10, 9, 8, 8]
                }
            }
    
    async def get_yearly_sales_comparison(self, current_year: int = None, comparison_year: int = None) -> Dict[str, Any]:
        try:
            if not current_year:
                current_year = datetime.now().year
            if not comparison_year:
                comparison_year = current_year - 1

            spark_stats = await self.get_spark_analytics()
            monthly_data = spark_stats.get("monthly_receipts", [])
            
            current_year_sales = [0] * 12
            other_year_sales = [0] * 12
            
            for month_stat in monthly_data:
                if month_stat.get("month"):
                    try:
                        year_month = month_stat["month"]
                        year, month = map(int, year_month.split("-"))
                        revenue = int(month_stat.get("total_revenue", 0) / 1000)  # В тысячах
                        
                        if year == current_year and 1 <= month <= 12:
                            current_year_sales[month - 1] = revenue
                        elif year == comparison_year and 1 <= month <= 12:
                            other_year_sales[month - 1] = revenue
                    except (ValueError, KeyError):
                        continue
            
            return {
                "payload": {
                    "currentYear": current_year_sales,
                    "otherYear": other_year_sales
                }
            }
            
        except Exception as e:
            logger.error("Error getting yearly sales comparison", error=str(e))
            return {
                "payload": {
                    "currentYear": [120, 85, 93, 104, 150, 210, 164, 187, 213, 234, 256, 278],
                    "otherYear": [120, 11, 93, 120, 150, 288, 164, 187, 313, 234, 156, 278]
                }
            }
    
    async def cleanup(self):
        try:
            await self.db_service.close_session()
            self.spark_service.stop_spark()
            logger.info("Analytics service cleaned up")
        except Exception as e:
            logger.error("Error during cleanup", error=str(e))
