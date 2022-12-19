import sqlite3
import os

######################################################################################
# db: stock.db
#
# table:
#   t_return_equal_weight: (absolute) stock return for each quarter (equal weight)
######################################################################################

dbName = 'stock.db'
db_path = dbName
print(f'*** Using db: {dbName} ***\n\n')

RETURN_TABLE = 't_return_equal_weight'


connection = sqlite3.connect(db_path)
cursor = connection.cursor()

# cursor.execute(f"DROP TABLE {RETURN_TABLE};")

query = 'CREATE TABLE ' + RETURN_TABLE + ' (\
            Model TEXT, \
            Strategy TEXT, \
            Year INT, \
            Quarter INT, \
            "Stock Return" REAL, \
            PRIMARY KEY (Model, Strategy, Year, Quarter)\
            );'
cursor.execute(query)
connection.commit()
