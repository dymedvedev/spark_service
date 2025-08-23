from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional
from datetime import datetime
from app.services.analytics_service import AnalyticsService
from app.schemas.receipt import ReceiptCreate, AnalyticsResponse
import structlog

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1", tags=["analytics"])

analytics_service = AnalyticsService()


@router.get("/health")
async def health_check():
    """Проверка здоровья сервиса"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@router.get("/stats/basic")
async def get_basic_stats():
    """Получение базовой статистики"""
    try:
        stats = await analytics_service.get_basic_stats()
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error("Error getting basic stats", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/daily")
async def get_daily_stats(date: Optional[str] = None):
    """Получение дневной статистики"""
    try:
        stats = await analytics_service.get_daily_analytics(date)
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error("Error getting daily stats", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/monthly")
async def get_monthly_stats(year: Optional[int] = None, month: Optional[int] = None):
    """Получение месячной статистики"""
    try:
        stats = await analytics_service.get_monthly_report(year, month)
        return JSONResponse(content=stats)
    except Exception as e:
        logger.error("Error getting monthly stats", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/products")
async def get_product_analytics():
    """Получение аналитики по продуктам"""
    try:
        analytics = await analytics_service.get_product_analytics()
        return JSONResponse(content=analytics)
    except Exception as e:
        logger.error("Error getting product analytics", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/payments")
async def get_payment_analytics():
    """Получение аналитики по оплатам"""
    try:
        analytics = await analytics_service.get_payment_analytics()
        return JSONResponse(content=analytics)
    except Exception as e:
        logger.error("Error getting payment analytics", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/spark")
async def get_spark_analytics():
    """Получение Spark аналитики"""
    try:
        analytics = await analytics_service.get_spark_analytics()
        return JSONResponse(content=analytics)
    except Exception as e:
        logger.error("Error getting Spark analytics", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/receipts/process")
async def process_receipt(receipt: ReceiptCreate, background_tasks: BackgroundTasks):
    """Обработка входящего чека"""
    try:
        # Обработка в фоновом режиме
        background_tasks.add_task(analytics_service.process_receipt, receipt)
        
        return {
            "message": "Receipt queued for processing",
            "receipt_id": receipt.id,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error("Error processing receipt", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/monthly/sales")
async def get_monthly_sales(year: Optional[int] = None, month: Optional[int] = None):
    """Получение продаж за месяц по дням"""
    try:
        sales = await analytics_service.get_monthly_sales_by_days(year, month)
        return JSONResponse(content=sales)
    except Exception as e:
        logger.error("Error getting monthly sales", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/receipts/count")
async def get_receipts_count(year: Optional[int] = None, month: Optional[int] = None):
    """Получение количества чеков за месяц по дням"""
    try:
        count_data = await analytics_service.get_receipts_count_by_days(year, month)
        return JSONResponse(content=count_data)
    except Exception as e:
        logger.error("Error getting receipts count", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/stats/products/in-receipts")
async def get_products_in_receipts(year: Optional[int] = None, month: Optional[int] = None):
    """Получение количества товаров в чеках за месяц по дням"""
    try:
        products_data = await analytics_service.get_products_in_receipts_by_days(year, month)
        return JSONResponse(content=products_data)
    except Exception as e:
        logger.error("Error getting products in receipts", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/stats/yearly/comparison")
async def get_yearly_comparison(current_year: Optional[int] = None, comparison_year: Optional[int] = None):
    """Получение сравнения продаж по годам"""
    try:
        comparison_data = await analytics_service.get_yearly_sales_comparison(current_year, comparison_year)
        return JSONResponse(content=comparison_data)
    except Exception as e:
        logger.error("Error getting yearly comparison", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


