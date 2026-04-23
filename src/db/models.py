from datetime import date
from decimal import Decimal
from sqlalchemy import String, Date, Numeric, Integer
from sqlalchemy.orm import Mapped, mapped_column
from src.db.connection import Base


class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    product: Mapped[str] = mapped_column(String(200), nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    total: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    sale_date: Mapped[date] = mapped_column(Date, nullable=False)
    region: Mapped[str] = mapped_column(String(100), nullable=False)

    def __repr__(self):
        return f"<Sale order_id={self.order_id} product={self.product}>"
