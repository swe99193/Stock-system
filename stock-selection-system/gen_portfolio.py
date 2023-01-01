import pickle
import pandas as pd
import sqlite3
import os
import numpy as np
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('Year', type=str)
parser.add_argument('Quarter', type=str)
parser.add_argument('Model', type=str)
parser.add_argument('Strategy', type=str)
args = parser.parse_args()

Year = int(args.Year)
Quarter = int(args.Quarter)
Model = args.Model
Strategy = args.Strategy

PATH_pred = os.path.join("predictions")

RETURN_TABLE = 't_return_equal_weight'
dbName = 'stock.db'
db_path = os.path.join('..', 'Database', dbName)
connection = sqlite3.connect(db_path)


def retrieve_stock_return(co_id_list, Year, Quarter):
    ''' Given company list, return each (absolute) stock return '''
    cursor = connection.cursor()

    return_list = list()

    for co_id in co_id_list:
        query = f'SELECT "Stock Return" FROM t_final_data WHERE CO_ID="{co_id}" and Year={Year} and Quarter={Quarter}'
        return_list.append(cursor.execute(query).fetchone()[0])

    return return_list


def gen_equal_weight_return(Year, Quarter, Model):
    '''
        write into t_return_equal_weight @ portfolio_return.db
    '''
    strategy="equal weight"

    cursor = connection.cursor()

    with open(os.path.join(PATH_pred, Model, f"{Year}-{Quarter}-company-rank.pickle"), "rb") as fp:
        co_id_rank = pickle.load(fp)

    # select top 25 companies
    co_id_list = co_id_rank[:25]

    # collect (absolute) stock return from t_final_data @ stock.db
    return_list = retrieve_stock_return(co_id_list, Year, Quarter)

    # equal weight
    stock_return = np.mean(return_list)

    # overwrite existing row or insert a new row (sqlite)
    if cursor.execute(f'SELECT * FROM {RETURN_TABLE} WHERE Model = "{Model}" AND Strategy = "{strategy}" AND Year = {Year} AND Quarter = {Quarter}').fetchone() is None:
        data = Model, strategy, Year, Quarter, stock_return
        query = f'INSERT INTO {RETURN_TABLE} (Model, Strategy, Year, Quarter, "Stock Return") VALUES (?,?,?,?,?);'
        print('[QUERY] ' + query) 
        print('[DATA] ', data) 
        cursor.execute(query, data)
    else:
        query = f'UPDATE {RETURN_TABLE} \
            SET "Stock Return" = {stock_return} \
            WHERE \
                Model = "{Model}" AND Strategy = "{strategy}" AND Year = {Year} AND Quarter = {Quarter} \
            '
        print('[QUERY] ' + query) 
        cursor.execute(query)

    connection.commit()


def gen_portfolio_return(Year, Quarter, Model, Strategy):
    if Strategy == "equal weight":
        gen_equal_weight_return(Year, Quarter, Model)
    else:
        raise Exception("[ERROR] we don't have this strategies yet...")

def main():
    gen_portfolio_return(Year, Quarter, Model, Strategy)


if __name__=="__main__":
    main()