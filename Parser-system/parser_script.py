# Description: 
#   scripts to parse financial statements & calculate ratios

import os
import subprocess
import sqlite3
import financial_info_parser_lib
import ratio_generator_lib
from time import sleep
import pandas as pd
from argparse import ArgumentParser
from CustomException import RequestError

DB_NAME = 'stock.db'
db_path = os.path.join('..', 'Database', DB_NAME)
print(f'✅ *** Using db: {DB_NAME} ***\n\n')
SLEEP_TIME = 0  # seconds

INPUT_DF = pd.read_csv(os.path.join('..', 'csv', 'target_company.csv'), dtype="str") # target company ids


def data_pipeline(param, connection, parse, calc):
    '''
        parse data + calculate indices
    '''
    if parse == True:
        financial_info_parser_lib.parse_func(param, connection)
    
    if calc == True:
        ratio_generator_lib.generate_final_data(param, connection)
    return


def main():
    parser = ArgumentParser()
    parser.add_argument("year", help="target year", type=int)
    parser.add_argument("quarter", help="target quarter", type=int)
    parser.add_argument("--target", help="target company id", dest="company_id", type=str)
    parser.add_argument('--parse', help="parse data (財報)", action='store_true')
    parser.add_argument('--calc', help="calculate financial indices", action='store_true')

    # argument format
    args = parser.parse_args()
    year = args.year
    quarter = args.quarter
    id = args.company_id
    parse = args.parse
    calc = args.calc

    if(not parse and not calc):
        print("❓ no parse and no calc")
        return

    connection = sqlite3.connect(db_path)


    # parse one company
    if id:
        print(f'🚀 Current target: {id}, {year}, {quarter}\n')
        param = (id, str(year), str(quarter))   # id without .TW
        data_pipeline(param, connection, parse, calc)

    # parse all companies
    else:

        CO_ID_list = INPUT_DF['CO_ID'].values
        print(f'🟢 Target list: {CO_ID_list}\n')
        
        for id in CO_ID_list:
            print(f'🚀 Current target: {id}, {year}, {quarter}\n')
            
            param = (id, str(year), str(quarter))
            data_pipeline(param, connection, parse, calc)
            
            sleep(SLEEP_TIME)
    
    connection.close()
    return


if __name__ == '__main__':
    main()