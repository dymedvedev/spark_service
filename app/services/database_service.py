import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.models.receipt import Check, Item, User, Organization, Invoice
from app.schemas.receipt import ReceiptCreate
from app.models.database import AsyncSessionLocal

logger = structlog.get_logger()


class DatabaseService:
    def __init__(self):
        self.session: AsyncSession = None
        
    async def get_session(self):
        if not self.session:
            self.session = AsyncSessionLocal()
        return self.session
        
    async def get_new_session(self):
        return AsyncSessionLocal()
        
    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None
            
    async def save_receipt(self, receipt: ReceiptCreate, use_new_session: bool = False) -> bool:
        try:
            if use_new_session:
                session = await self.get_new_session()
            else:
                session = await self.get_session()

            check = Check(
                check_id=receipt.id,
                check_sum=receipt.total_sum or sum(item.sum for item in receipt.items),
                created_at=receipt.created_at
            )
            session.add(check)
            await session.flush()

            user = User(
                user_id=receipt.user
            )
            session.add(user)
            await session.flush()
            check.user_id = user.user_id

            if receipt.items:
                first_item = receipt.items[0]
                org_name, legal_form = self._parse_organization(first_item.organization_form)
                
                organization = Organization(
                    org_name=org_name,
                    legal_form=legal_form
                )
                session.add(organization)
                await session.flush()
                check.org_id = organization.org_id

                if first_item.invoice_type and first_item.invoice_sum:
                    invoice = Invoice(
                        invoice_sum=first_item.invoice_sum,
                        invoice_name=first_item.name,
                        payment_type=first_item.payment_type
                    )
                    session.add(invoice)
                    await session.flush()
                    check.invoice_id = invoice.invoice_id

                for item in receipt.items:
                    db_item = Item(
                        item_name=item.name,
                        item_price=item.price,
                        item_type=item.product_type,
                        item_quantity=item.quantity,
                        item_sum=item.sum,
                        check_id=check.check_id
                    )
                    session.add(db_item)
                    
            check.user_id = user.user_id
            
            await session.commit()

            if use_new_session:
                await session.close()
                
            logger.info("Receipt saved successfully", receipt_id=receipt.id)
            return True
            
        except Exception as e:
            await session.rollback()

            if use_new_session:
                await session.close()
                
            logger.error("Error saving receipt", error=str(e), receipt_id=receipt.id)
            return False
            
    def _parse_organization(self, org_form: str) -> tuple:
        if '"' in org_form:
            parts = org_form.split('"')
            if len(parts) >= 3:
                return parts[1], parts[2].strip()
        return org_form, "ООО"
        
    async def get_receipt_stats(self):
        try:
            session = await self.get_session()

            total_receipts = await session.scalar(
                select(func.count(Check.check_id))
            )

            avg_receipt = await session.scalar(
                select(func.avg(Check.check_sum))
            )

            total_revenue = await session.scalar(
                select(func.sum(Check.check_sum))
            )

            monthly_receipts = await session.scalar(
                select(func.count(Check.check_id))
                .where(func.date_trunc('month', Check.created_at) == 
                      func.date_trunc('month', func.now()))
            )
            
            return {
                "total_receipts": total_receipts or 0,
                "avg_receipt": float(avg_receipt) if avg_receipt else 0,
                "total_revenue": float(total_revenue) if total_revenue else 0,
                "monthly_receipts": monthly_receipts or 0
            }
            
        except Exception as e:
            logger.error("Error getting receipt stats", error=str(e))
            return {}
            
    async def get_payment_types_stats(self):
        try:
            session = await self.get_session()
            
            result = await session.execute(
                select(Invoice.payment_type, func.count(Invoice.payment_type))
                .group_by(Invoice.payment_type)
            )
            
            payment_types = {}
            for payment_type, count in result:
                if payment_type:
                    payment_types[payment_type] = count
                    
            return payment_types
            
        except Exception as e:
            logger.error("Error getting payment types stats", error=str(e))
            return {}
