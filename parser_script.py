import os
import subprocess
import sqlite3
import automated_parser_2019
import automated_parser_2013
import automated_parser_2009
import financial_info_parser_lib
import ratio_generator_lib
from time import sleep

root_path = 'ratios'

def parse(param, connection):
    year = int(param[1])
    try: 
        # if year >= 2019:
        #     automated_parser_2019.parse_func(param, connection)
        # elif 2013 <= year <= 2018:
        #     automated_parser_2013.parse_func(param, connection)
        # elif 2009 <= year <= 2012:
        #     raise Exception('year value not supported, in dev process')
        # else:
        #     raise Exception('year value not supported')
        if 2013 <= year:
            parse_func = financial_info_parser_lib.parse_func
        elif 2009 <= year <= 2012:
            print('year value not supported, in dev process')
            return
        else:
            print('year value not supported')
            return
        parse_func(param, connection)
    # try again if the data can not successfully parsed or other error occur
    except:
        sleep(5)
        parse(param, connection)
        

def generate_ratio(param, connection):
    ratio_generator_lib.generate_ratios(param, connection)


def main():
    # return

    # input:
    # (stock id, year, season)
    query_param = [
                    # ('2330', '2021', '1'),
                #    ('2330', '2021', '2'),
                # #    ('2330', '2021', '3'),
                #    ('2330', '2021', '4'),
                   ]

    if root_path not in os.listdir():
        os.mkdir(root_path)

    connection = sqlite3.connect(os.path.join(root_path, 'stock.db'))

    # TODO: parallel processing
    # for year in range(2019, 2022):
    #     for season in range(1,5)
    #     parse(('2330', str(year), str(season)), connection)

    # for year in range(2019, 2023):
        # parse(('2330', str(year), '4'), connection)
    
    for year in range(2018, 2020):
        for season in range(1,5):
            parse(('2887', str(year), str(season)), connection)
            ratio_generator_lib.generate_final_data(('2887', str(year), str(season)), connection)

    # parse(('2330', str('2013'), '2'), connection)
    
    print('\n---------------- Complete ----------------')
    connection.close()
    return


if __name__ == '__main__':
    main()