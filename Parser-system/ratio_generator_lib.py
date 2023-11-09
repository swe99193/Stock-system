from collections import defaultdict
import traceback
import pandas as pd
import os
import subprocess
import sqlite3
import yfinance as yf

# important index for calculating ratios
CodeMap = {
        '1170': 'Receivables', # 應收帳款
        '11XX': 'Total Current Asset', # 流動資產
        '130X': 'Inventory', # 存貨
        '15XX': 'Total Non-Current Assets', # 非流動資產
        '1600': '不動產', # 不動產、廠房及設備
        '1780': 'Intangible Assets', # 無形資產
        '1900': 'Other Non-Current Assets', # 其他非流動資產
        '1XXX': 'Total Assets', # 資產總計
        '21XX': 'Total Current Liabilities', # 流動負債
        '25XX': 'Total Non-Current Liabilities', # 非流動負債
        '2600': 'Other Non-Current Liabilities', # 其他非流動負債
        '3XXX': 'Total Equity', # 權益總額
        '3100': 'Shareholder Equity', # 股本合計
        '3120': 'Preferred Stock', # 特別股股本
        '4000': 'Revenue', # 營業收入
        '5000': 'Cost Of Goods Sold', # 營業成本
        '5900': 'Gross Profit', # 營業毛利
        '6900': 'Operating Income', # 營業利益
        '7900': 'Pre-Tax Income', # 繼續營業單位稅前淨利
        '8200': 'Net Income', # 本期淨利
        'AAAA': 'Operating Cash Flow', # 營業活動之淨現金流入
        'B02700': 'Capital Expenditures' # 取得不動產、廠房及設備
        }
FinanceCompanyCodeMap = {
        '1170': 'Receivables',
        '1780': 'Intangible Assets', 
        '1XXX': 'Total Assets',
        '3100': 'Shareholder Equity',
        '3XXX': 'Total Equity',
        '3120': 'Preferred Stock',
        '7900': 'Pre-Tax Income',
        '8200': 'Net Income',
        'AAAA': 'Operating Cash Flow',
        'B02700': 'Capital Expenditures'
        }

table_name = lambda param: '_'.join(['t', param[0], param[1], param[2]])

# INF = 100000000

# 季的定義是根據財報公佈時間，所以第一季是5月中～8月中左右，我們有再向後取個5天左右避免遇到假日之類的
Q1_START = ('-05-20', '-05-25')
Q1_END = ('-08-19', '-08-24')
Q2_START = ('-08-20', '-08-25')
Q2_END = ('-11-19', '-11-24')
Q3_START = ('-11-20', '-11-25')
Q3_END = ('-02-19', '-02-27')
Q4_START = ('-02-20', '-02-27')
Q4_END = ('-05-19', '-05-24')

BASE_ID = '0050.TW' # 大盤 id

INPUT_DF = pd.read_csv(os.path.join('..', 'csv', 'target_company.csv'), dtype="str") # target company ids

FINAL_TABLE = 't_final_data'

logger = open(os.path.join('log', "ratio_generator.txt"), "a")


def _stock_price_helper(id, Y, date_suffix):
    '''
        收盤價 Close
    '''
    df = yf.download(id, start=Y+date_suffix[0], end=Y+date_suffix[-1])
    return df['Close'][0] # take the first available row


def stock_return_parser(param, cursor):
    '''
        Given company id, year and quarter, calculate the stock return and store it into sqlite table

        param: (id, Year, Quarter)
            id(str): id of target company
            Year(str)
            Quarter(str)
        cursor: sqlite cursor

        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: company id should NOT be provided with ".TW" suffix
    '''
    id, Year, Quarter = param
    query_id = id + '.TW'

    growth_rate = lambda start, end: (end - start)/start
    year_increment = lambda year: str(int(year) + 1) # year increment in string

    if Quarter == '1':
        start = _stock_price_helper(query_id, Year, Q1_START)    # 個股收盤價
        end = _stock_price_helper(query_id, Year, Q1_END)
        # b_start = _stock_price_helper(BASE_ID, Year, Q1_START) # 大盤收盤價
        # b_end = _stock_price_helper(BASE_ID, Year, Q1_END)
    elif Quarter == '2':
        start = _stock_price_helper(query_id, Year, Q2_START)
        end = _stock_price_helper(query_id, Year, Q2_END)
        # b_start = _stock_price_helper(BASE_ID, Year, Q2_START)
        # b_end = _stock_price_helper(BASE_ID, Year, Q2_END)
    elif Quarter == '3':
        start = _stock_price_helper(query_id, Year, Q3_START)
        end = _stock_price_helper(query_id, year_increment(Year), Q3_END)
        # b_start = _stock_price_helper(BASE_ID, Year, Q3_START)
        # b_end = _stock_price_helper(BASE_ID, incr(Year), Q3_END)
    elif Quarter == '4':
        start = _stock_price_helper(query_id, year_increment(Year), Q4_START)
        end = _stock_price_helper(query_id, year_increment(Year), Q4_END)
        # b_start = _stock_price_helper(BASE_ID, incr(Year), Q4_START)
        # b_end = _stock_price_helper(BASE_ID, incr(Year), Q4_END)
    else:
        raise Exception(f"[ERROR] illegal Quarter: {Quarter}")

    # (changed to 絕對return)
    # 相對return = 個股 - 大盤
    # result = growth_rate(start, end) - growth_rate(b_start, b_end)

    # 絕對stock return
    result = growth_rate(start, end)

    # update column "Stock Return" (sqlite)
    query = f'UPDATE {FINAL_TABLE} \
            SET "Stock Return" = {result} \
            WHERE \
                CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}" \
            '
    print('[QUERY] ' + query) 
    cursor.execute(query)


def generate_ratios(param, cursor):
    '''
        Generate financial indices from financial statements (from parser). 
        
        Different calculations for general companies & financial companies

        param (str, str, str): (stock id, year, season) 
        cursor: sqlite cursor
        
        Note: If you call this function with the same parameters again, the original data will be replaced by new data.

        Note: company id should NOT be provided with ".TW" suffix
    '''
    table = table_name(param)
    id, Year, Quarter = param
    sector = INPUT_DF[INPUT_DF['CO_ID'] == id]['Sector'].values[0]

    val = {}

    codeMap = CodeMap if sector != 'Financial Services' else FinanceCompanyCodeMap
    missing_columns = set()

    for code, name in codeMap.items():
        query = f'SELECT Money FROM {table} WHERE Code = "{code}"'
        result = cursor.execute(query).fetchone()
        
        # check missing columns, and set zero for missing value
        if result is None:   
            # 忽略檢查: 特別股股本 '3120', 無形資產 '1780'
            missing_columns.add(code)
            val[name] = 0
        else:
            val[name] = result[0]

    if missing_columns:
        warning = f'[WARNING] 🔴 In {table} ,these columns are missing: {missing_columns}\n'
        logger.write(warning)
        print(warning)
    
    # if分母0, set zero
    divide = lambda dividend, divisor: dividend / divisor if divisor != 0 else 0

    if sector != 'Financial Services':
        val['Long Term Debt'] = val['Total Non-Current Liabilities'] - val['Other Non-Current Liabilities']
        val['Long Term Investment'] = val['Total Non-Current Assets'] - val['不動產'] - val['Intangible Assets'] - val['Other Non-Current Assets']
        val['Shares Outstanding'] = val['Shareholder Equity'] / 10    # 股本/10
        
        ratios = dict()

        ratios["Current Ratio"] = divide(val['Total Current Asset'], val['Total Current Liabilities'])
        ratios["Long Term Debt to Capital Ratio"] = divide(val['Long Term Debt'], val['Long Term Debt'] + val['Total Equity'])
        ratios["Debt to Equity Ratio"] = divide(val['Long Term Debt'], val['Total Equity'])
        ratios["Gross Margin"] = divide(val['Gross Profit'], val['Revenue'])
        ratios["Operating Margin"] = divide(val['Operating Income'], val['Revenue'])
        ratios["Pre-Tax Profit Margin"] = divide(val['Pre-Tax Income'], val['Revenue'])
        ratios["Net Profit Margin"] = divide(val['Net Income'], val['Revenue'])
        ratios["Asset Turnover"] = divide(val['Revenue'], val['Total Assets'])
        ratios["Inventory Turnover Ratio"] = divide(val['Cost Of Goods Sold'], val['Inventory'])
        ratios["Receivable Turnover"] = divide(val['Revenue'], val['Receivables'])
        ratios["Days Sales In Receivables"] = divide(val['Receivables'], val['Revenue']) * 365
        ratios["ROE"] = divide(val['Net Income'], val['Total Equity'])
        ratios["ROTE"] = divide(val['Net Income'], val['Total Equity'] - val['Intangible Assets'] - val['Preferred Stock'])
        ratios["ROA"] = divide(val['Net Income'], val['Total Assets'])
        ratios["ROI"] = divide(val['Net Income'], val['Long Term Debt'] + val['Long Term Investment'])
        ratios["Book Value Per Share"] = divide(val['Total Equity'] - val['Preferred Stock'], val['Shares Outstanding'])
        ratios["Operating Cash Flow Per Share"] = divide(val['Operating Cash Flow'], val['Shares Outstanding'])
        ratios["Free Case Flow Per Share"] = divide(val['Operating Cash Flow'] - val['Capital Expenditures'], val['Shares Outstanding'])

    else:
        val['Shares Outstanding'] = val['Shareholder Equity'] / 10    # 股本/10
        
        ratios = dict()

        ratios["Current Ratio"] = None
        ratios["Long Term Debt to Capital Ratio"] = None
        ratios["Debt to Equity Ratio"] = None
        ratios["Gross Margin"] = None
        ratios["Operating Margin"] = None
        ratios["Pre-Tax Profit Margin"] = None
        ratios["Net Profit Margin"] = None
        ratios["Asset Turnover"] = None
        ratios["Inventory Turnover Ratio"] = None
        ratios["Receivable Turnover"] = None
        ratios["Days Sales In Receivables"] = None
        ratios["ROE"] = divide(val['Net Income'], val['Total Equity'])
        ratios["ROTE"] = divide(val['Net Income'], val['Total Equity'] - val['Intangible Assets'] - val['Preferred Stock'])
        ratios["ROA"] = divide(val['Net Income'], val['Total Assets'])
        ratios["ROI"] = None
        ratios["Book Value Per Share"] = divide(val['Total Equity'] - val['Preferred Stock'], val['Shares Outstanding'])
        ratios["Operating Cash Flow Per Share"] = divide(val['Operating Cash Flow'], val['Shares Outstanding'])
        ratios["Free Case Flow Per Share"] = divide(val['Operating Cash Flow'] - val['Capital Expenditures'], val['Shares Outstanding'])

    # load ratios into sqlite table
    # construct update query
    query = f'UPDATE {FINAL_TABLE} SET '
    
    for i, (key, val) in enumerate(ratios.items()):
        if val is None:
            query += f'"{key}" = NULL'  # set NULL
        else:
            query += f'"{key}" = {val}'
        if i<len(ratios)-1: query += ', '

    query += f' WHERE CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}"'
    print('[INFO] query: ' + query) 
    cursor.execute(query)


def generate_final_data(param, connection):
    '''
        Generate financial indices + stock return. 

        param (str, str, str): (stock id, year, season) 
        cursor: sqlite cursor

        Note: company id should NOT be provided with ".TW" suffix
    '''
    cursor = connection.cursor()
    id, Year, Quarter = param

    print(f'[INFO] 🟢 Start ratio generator')

    # initialize the row if not exists in final table (sqlite)
    if cursor.execute(f'SELECT * FROM {FINAL_TABLE} WHERE CO_ID = "{id}" AND Year = "{Year}" AND Quarter = "{Quarter}"').fetchone() is None:
        sector = INPUT_DF[INPUT_DF['CO_ID'] == id]['Sector'].values[0]
        data = id, Year, Quarter, sector

        query = f'INSERT INTO {FINAL_TABLE} (CO_ID, Year, Quarter, Sector) VALUES (?,?,?,?);'
        print('[INFO] query: ' + query) 
        cursor.execute(query, data)

    try:
        stock_return_parser(param, cursor)
        generate_ratios(param, cursor)
        
    except Exception as e:
        error_message = f'[ERROR] ❌ generate ratio: {table_name((id, Year, Quarter))}\n'
        logger.write(error_message)
        print(error_message)
        traceback.print_exc()
        # connection.rollback() # not needed
        return


    connection.commit()
    print(f'\n[INFO] ✅ Complete final data: {id}, {Year}, {Quarter}')
    print('----------------------------------------------------------\n')
    return
