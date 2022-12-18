import sqlite3
import os

######################################################################################
# db: portfolio_return.db
#
# table:
#   t_return_equal_weight: (absolute) stock return for each quarter (equal weight)
######################################################################################

dbName = 'portfolio_return.db'
db_path = os.path.join('..', 'Database', dbName)
print(f'*** Using db: {dbName} ***\n\n')

RETURN_TABLE = 't_return_equal_weight'

connection = sqlite3.connect(db_path)
cursor = connection.cursor()

query = 'CREATE TABLE ' + RETURN_TABLE + ' (\
            Model TEXT, \
            Strategy TEXT, \
            Year INT, \
            Quarter INT, \
            "Stock Return" REAL, \
            PRIMARY KEY (Year, Quarter)\
            );'
cursor.execute(query)
connection.commit()
