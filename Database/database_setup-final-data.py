import sqlite3
import os

######################################################################################
# db: stock.db
#
# table:
#   t_final_data: store sector & financial ratios & (absolute) stock return (for each company, year, quarter)
######################################################################################

dbName = 'stock.db'
db_path = dbName
print(f'*** Using db: {dbName} ***\n\n')

FINAL_TABLE = 't_final_data'

connection = sqlite3.connect(db_path)
cursor = connection.cursor()

# cursor.execute(f"DROP TABLE {FINAL_TABLE};")

query = 'CREATE TABLE ' + FINAL_TABLE + ' (\
            CO_ID TEXT, \
            Year INT, \
            Quarter INT, \
            Sector TEXT, \
            "Current Ratio" REAL, \
            "Long Term Debt to Capital Ratio" REAL, \
            "Debt to Equity Ratio" REAL, \
            "Gross Margin" REAL, \
            "Operating Margin" REAL, \
            "Pre-Tax Profit Margin" REAL, \
            "Net Profit Margin" REAL, \
            "Asset Turnover" REAL, \
            "Inventory Turnover Ratio" REAL, \
            "Receivable Turnover" REAL, \
            "Days Sales In Receivables" REAL, \
            "ROE" REAL, \
            "ROTE" REAL, \
            "ROA" REAL, \
            "ROI" REAL, \
            "Book Value Per Share" REAL, \
            "Operating Cash Flow Per Share" REAL, \
            "Free Case Flow Per Share" REAL, \
            "Stock Return" REAL, \
            PRIMARY KEY (CO_ID, Year, Quarter)\
            );'
cursor.execute(query)
connection.commit()
