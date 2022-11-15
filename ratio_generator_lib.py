from collections import defaultdict
import pandas as pd
import os
import subprocess
import sqlite3
import yfinance as yf

# important index for calculating ratios
CodeSet = {
        '1170',
        '11XX',
        '130X',
        '15XX',
        '1600',
        '1780',
        '1900',
        '1XXX',
        '21XX',
        '25XX',
        '2600',
        '3XXX',
        '3100',
        '3120',
        '4000',
        '5000',
        '5900',
        '6900',
        '7900',
        '8200',
        # 'A20100',
        # 'A20200',
        'AAAA',
        'B02700'}
# (for reference)
FinCompCodeSet = {
        '1170',
        '1780',
        '1XXX',
        '3100',
        '3XXX',
        '7900',
        '8200',
        'AAAA',
        'B02700'}

table_name = lambda param: '_'.join(['t', param[0], param[1], param[2]])


# 季的定義是根據財報公佈時間，所以第一季是5月中～8月中左右，我們有再向後取個5天左右避免遇到假日之類的
Q1_START = ('-05-20', '-05-25')
Q1_END = ('-08-19', '-08-24')
Q2_START = ('-08-20', '-08-25')
Q2_END = ('-11-19', '-11-24')
Q3_START = ('-11-20', '-11-25')
Q3_END = ('-02-19', '-02-27')
Q4_START = ('-04-05', '-04-13')
Q4_END = ('-07-04', '-07-09')

BASE_ID = '0050.TW' # 大盤 id

INPUT_DF = pd.read_csv('input.csv') # target company ids

FINAL_TABLE = 't_final_data'


def _stock_price_helper(id, Y, date_suffix):
    '''
        收盤價 Close
    '''
    df = yf.download(id, start=Y+date_suffix[0], end=Y+date_suffix[-1])
    return df['Close'][0] # take the first available row


def stock_return_parser(param, connection):
    '''
        Given company id, year and quarter, calculate the stock return and store it into sqlite table

        param: (id, Year, Quarter)
            id(str): id of target company
            Year(str)
            Quarter(str)
        connection: sqlite

        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: company id SHOULD be provided with ".TW" suffix
    '''
    cursor = connection.cursor()
    id, Year, Quarter = param
    id += '.TW'

    print(f'\nCalculating Stock Return of {id}, {Year}, {Quarter}')

    growth_rate = lambda start, end: (end - start)/start
    incr = lambda Y: str(int(Year) + 1) # year increment in string

    if Quarter == '1':
        start = _stock_price_helper(id, Year, Q1_START)    # 個股收盤價
        end = _stock_price_helper(id, Year, Q1_END)
        b_start = _stock_price_helper(BASE_ID, Year, Q1_START) # 大盤收盤價
        b_end = _stock_price_helper(BASE_ID, Year, Q1_END)
    elif Quarter == '2':
        start = _stock_price_helper(id, Year, Q2_START)
        end = _stock_price_helper(id, Year, Q2_END)
        b_start = _stock_price_helper(BASE_ID, Year, Q2_START)
        b_end = _stock_price_helper(BASE_ID, Year, Q2_END)
    elif Quarter == '3':
        start = _stock_price_helper(id, Year, Q3_START)
        end = _stock_price_helper(id, incr(Year), Q3_END)
        b_start = _stock_price_helper(BASE_ID, Year, Q3_START)
        b_end = _stock_price_helper(BASE_ID, incr(Year), Q3_END)
    else:   # (Q == '4')
        start = _stock_price_helper(id, incr(Year), Q4_START)
        end = _stock_price_helper(id, incr(Year), Q4_END)
        b_start = _stock_price_helper(BASE_ID, incr(Year), Q4_START)
        b_end = _stock_price_helper(BASE_ID, incr(Year), Q4_END)

    # 相對return = 個股 - 大盤
    result = growth_rate(start, end) - growth_rate(b_start, b_end)

    # update column "Stock Return" (sqlite)
    query = f'UPDATE {FINAL_TABLE} \
            SET "Stock Return" = {result} \
            WHERE \
                CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}" \
            '
    print('[QUERY] ' + query) 
    cursor.execute(query)
    connection.commit()



def generate_ratios(param, connection):
    '''
        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: company id should NOT be provided with ".TW" suffix
    '''
    cursor = connection.cursor()
    table = table_name(param)
    id, Year, Quarter = param
    id += '.TW'

    print(f'\nGenerating Ratios of {id}, {Year}, {Quarter}')

    val = defaultdict(int)

    for code in CodeSet:
        query = f'SELECT Money FROM {table} WHERE Code = "{code}"'
        result = cursor.execute(query).fetchone()
        
        # ignore codes that do not exist
        if result is not None:
            val[code] = result[0]

    val['Long Term Debt'] = val['25XX'] - val['2600'] if val['25XX']!=0 else 0
    val['Long Term Investment'] = val['15XX'] - val['1600'] - val['1780'] - val['1900'] if val['15XX']!=0 and val['1600']!=0 else 0
    val['Shares Outstanding'] = val['3100'] / 10    # 股本/10
    
    ratios = defaultdict(int)

    ratios["Current Ratio"] = val['11XX'] / val['21XX'] if val['21XX']!=0 else 0
    ratios["Long Term Debt to Capital Ratio"] = val['Long Term Debt']/(val['Long_Term_Debt'] + val['3XXX'])
    ratios["Debt to Equity Ratio"] = val['Long Term Debt'] / val['3XXX'] if val['Long Term Debt']!=0 else 0
    ratios["Gross Margin"] = val['5900'] / val['4000'] if val['4000']!=0 else 0
    ratios["Operating Margin"] = val['6900'] / val['4000'] if val['4000']!=0 else 0
    ratios["Pre-Tax Profit Margin"] = val['7900'] / val['4000'] if val['4000']!=0 else 0
    ratios["Net Profit Margin"] = val['8200'] / val['4000'] if val['4000']!=0 else 0
    ratios["Asset Turnover"] = val['4000'] / val['1XXX'] if val['1XXX']!=0 else 0
    ratios["Inventory Turnover Ratio"] = val['5000'] / val['130X'] if val['130X']!=0 else 0
    ratios["Receivable Turnover"] = val['4000'] / val['1170'] if val['1170']!=0 else 0
    ratios["Days Sales In Receivables"] = val['1170'] / val['4000'] * 365 if val['4000']!=0 else 0
    ratios["ROE"] = val['8200'] / val['3XXX'] if val['3XXX']!=0 else 0
    ratios["ROTE"] = val['8200'] / (val['3XXX'] - val['1780'] - val['3120']) if val['3XXX']!=0 else 0
    ratios["ROA"] = val['8200'] / val['1XXX'] if val['1XXX']!=0 else 0
    ratios["ROI"] = val['8200'] / (val['Long Term Debt'] + val['Long Term Investment']) if val['Long Term Debt']!=0 else 0
    ratios["Book Value Per Share"] = (val['3XXX'] - val['3120'])/ val['Shares Outstanding'] if val['Shares Outstanding']!=0 else 0
    ratios["Operating Cash Flow Per Share"] = val['AAAA'] / val['Shares Outstanding'] if val['Shares Outstanding']!=0 else 0
    ratios["Free Case Flow Per Share"] = (val['AAAA'] - val['B02700']) / val['Shares Outstanding'] if val['Shares Outstanding']!=0 else 0

    # load ratios into sqlite table
    query = f'UPDATE {FINAL_TABLE} SET '
    
    for i, (key, val) in enumerate(ratios.items()):
        query += f'"{key}" = {val}'
        if i<len(ratios)-1: query += ', '

    query += f' WHERE CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}"'
    print('[QUERY] ' + query) 
    cursor.execute(query)
    connection.commit()



def generate_final_data(param, connection):
    cursor = connection.cursor()
    id, Year, Quarter = param
    id += '.TW'

    print('----------------------------------------------------------')
    print(f'Generating final data: {id}, {Year}, {Quarter}')

    # initialize the row if not exists in final table (sqlite)
    if cursor.execute(f'SELECT * FROM {FINAL_TABLE} WHERE CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}"').fetchone() is None:
        sector = INPUT_DF[INPUT_DF['CO_ID'] == id]['Sector'].values[0]
        data = id, Year, Quarter, sector

        query = f'INSERT INTO {FINAL_TABLE} (CO_ID, Year, Quarter, Sector) VALUES (?,?,?,?);'
        print('[QUERY] ' + query) 
        cursor.execute(query, data)

        connection.commit()

    stock_return_parser(param, connection)
    generate_ratios(param, connection)

    print(f'\nComplete final data: {id}, {Year}, {Quarter}')
    print('----------------------------------------------------------\n')
