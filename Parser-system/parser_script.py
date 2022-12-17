import os
import subprocess
import sqlite3
import financial_info_parser_lib
import ratio_generator_lib
from time import sleep
import pandas as pd
from argparse import ArgumentParser
from CustomException import RequestError

# dbName = 'stock.db'
db_path = os.path.join('..', 'Database', dbName)
print(f'*** Using db: {dbName} ***\n\n')
SLEEP_TIME = 30
skip_parse = True # skip parsing web data

def parse(param, connection):
    if skip_parse:
        return 

    year = int(param[1])
    try: 
        if 2013 <= year:
            parse_func = financial_info_parser_lib.parse_func
        else:
            print('year value not supported, need validation to prevent breaking the database')
            return
        parse_func(param, connection)
    # try again if the data can not successfully parsed or other error occur
    except RequestError:
        sleep(SLEEP_TIME)
        parse(param, connection)
    except:
        raise
        

def generate_ratio(param, connection):
    ratio_generator_lib.generate_ratios(param, connection)


def main():
    # param format:
    # (stock id, year, season)
    query_param = [
                    # ('2330', '2021', '1'),
                #    ('2330', '2021', '2'),
                # #    ('2330', '2021', '3'),
                #    ('2330', '2021', '4'),
                   ]

    DEV_MODE = False
    MODE = None

    parser = ArgumentParser()
    parser.add_argument("-b", "-batch", help="define a range of target companies to parse from 2013 to now, see index in target_company.csv", dest="batch", type=int, nargs=2)
    parser.add_argument("-t", help="define the company id, year, quarter", dest="param_t", type=int, nargs=3)
    parser.add_argument("-y", help="define the company id, year", dest="param_y", type=int, nargs=2)
    parser.add_argument("-r", help="define the company id, start year, end year", dest="param_r", type=int, nargs=3)
    parser.add_argument("-a", "-all", help="define the target company, start from a year to now", dest="param_a", type=int, nargs=2)
    # parser.add_argument("-auto", help="define the target company, start from a year to now", dest="param_a", type=int, nargs=2)
    
    args = parser.parse_args()

    
    if args.batch:
        co_start, co_end = tuple(args.batch)
        MODE = 1
    elif args.param_t:
        id, year, quarter = tuple(args.param_t)
        MODE = 2
    elif args.param_y:
        id, year = tuple(args.param_y)
        MODE = 3
    elif args.param_r:
        id, year_start, year_end = tuple(args.param_r)
        MODE = 4
    elif args.param_a:
        id, year_start = args.param_a
        MODE = 5
    else:
        DEV_MODE = True


    connection = sqlite3.connect(db_path)
    
    if DEV_MODE:
        # manual configuration
        # id = '1102.TW'
        # for year in range(2009, 2023):
        #     for quarter in range(1,5):
        #         if year == 2022 and quarter == 2:
        #             break

        #         param = (id, str(year), str(quarter))
        #         parse(param, connection)

        #         ratio_generator_lib.generate_final_data(param, connection)
        pass

    elif MODE == 1:
        INPUT_DF = pd.read_csv('target_company.csv') # target company ids

        CO_ID_list = INPUT_DF.iloc[co_start : co_end+1]['CO_ID'].values
        print(f'Current target: {CO_ID_list}\n')
        for id in CO_ID_list:
            for year in range(2013, 2023):
                for quarter in range(1,5):
                    if year == 2022 and quarter == 2:
                        break
                    id = id.replace('.TW', '')  # remove .TW
                    param = (id, str(year), str(quarter))
                    parse(param, connection)

                    ratio_generator_lib.generate_final_data(param, connection)
    elif MODE == 2:        
        # parse one quarter
        print(f'Current target: {id}.TW, {year}, {quarter}\n')

        param = (str(id), str(year), str(quarter))   # id without .TW
        parse(param, connection)
        ratio_generator_lib.generate_final_data(param, connection)

    elif MODE == 3:
        # parse whole year
        for quarter in range(1,5):
            param = (str(id), str(year), str(quarter))   # id without .TW
            parse(param, connection)
            ratio_generator_lib.generate_final_data(param, connection)
    elif MODE == 4:
        # parse a range of years
        for year in range(year_start, year_end+1):
            for quarter in range(1,5):
                param = (str(id), str(year), str(quarter))   # id without .TW
                parse(param, connection)
                ratio_generator_lib.generate_final_data(param, connection)
    elif MODE == 5:
        # parse whole year until now
        for year in range(year_start, 2023):
            for quarter in range(1,5):
                if year == 2022 and quarter == 2:
                    break
                param = (str(id), str(year), str(quarter))   # id without .TW
                parse(param, connection)
                ratio_generator_lib.generate_final_data(param, connection)


    # parse(('2330', str('2013'), '2'), connection)
    
    connection.commit()
    connection.close()
    return


if __name__ == '__main__':
    main()