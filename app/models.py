from sqlalchemy import Column, TEXT, INT, REAL
from sqlalchemy.orm import relationship
from sqlalchemy import Index

from .database import Base


class Final_data(Base):
    __tablename__ = "t_final_data"

    CO_ID = Column(TEXT, primary_key=True)
    Year = Column(INT, primary_key=True)
    Quarter = Column(INT, primary_key=True)
    Sector = Column(TEXT)

    ratio1 = Column("Current Ratio", REAL)
    ratio2 = Column("Long Term Debt to Capital Ratio", REAL)
    ratio3 = Column("Debt to Equity Ratio", REAL)
    ratio4 = Column("Gross Margin", REAL)
    ratio5 = Column("Operating Margin", REAL)
    ratio6 = Column("Pre-Tax Profit Margin", REAL)
    ratio7 = Column("Net Profit Margin", REAL)
    ratio8 = Column("Asset Turnover", REAL)
    ratio9 = Column("Inventory Turnover Ratio", REAL)
    ratio10 = Column("Receivable Turnover", REAL)
    ratio11 = Column("Days Sales In Receivables", REAL)
    ratio12 = Column("ROE", REAL)
    ratio13 = Column("ROTE", REAL)
    ratio14 = Column("ROA", REAL)
    ratio15 = Column("ROI", REAL)
    ratio16 = Column("Book Value Per Share", REAL)
    ratio17 = Column("Operating Cash Flow Per Share", REAL)
    ratio18 = Column("Free Case Flow Per Share", REAL)

    Stock_Return = Column("Stock Return", REAL)

Index('main_index', Final_data.CO_ID, Final_data.Year, Final_data.Quarter)

class Return_equal_weight(Base):
    __tablename__ = "t_return_equal_weight"

    Model = Column(TEXT, primary_key=True)
    Strategy = Column(TEXT, primary_key=True)
    Year = Column(INT, primary_key=True)
    Quarter = Column(INT, primary_key=True)

    Stock_Return = Column("Stock Return", REAL)

Index('main_index', Return_equal_weight.Model, Return_equal_weight.Model, Return_equal_weight.Year, Return_equal_weight.Quarter)
