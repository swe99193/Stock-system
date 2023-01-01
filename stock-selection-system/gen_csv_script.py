# Note: require starting the data server (localhost)
# Note: reuse some parts from ratio-generator

import requests
import pandas as pd
import csv
import yfinance as yf
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('start_Year', type=str)
parser.add_argument('start_Quarter', type=str)
parser.add_argument('end_Year', type=str)
parser.add_argument('end_Quarter', type=str)
args = parser.parse_args()


start_Year = args.start_Year
start_Quarter = args.start_Quarter

end_Year = args.end_Year
end_Quarter = args.end_Quarter

PATH_TO_CSV = os.path.join('..', 'csv', 'ratio.csv')
INPUT_DF = pd.read_csv(os.path.join('..', 'csv', 'target_company.csv')) # target company ids
BASE_ID = '0050' # 大盤 id

# 大盤 stock return
base_return = dict()


title = {
    'CO_ID': 'CO_ID',
    'Name': 'Name',
    'Year': 'Year',
    'Quarter': 'Quarter',
    'Sector': 'Sector',
    'ratio1': 'Current Ratio',
    'ratio2': 'Long Term Debt / Captial',
    'ratio3': 'Debt / Equity',
    'ratio4': 'Gross Margin',
    'ratio5': 'Operating Margin',
    'ratio6': 'Pre-Tax Profit Margin',
    'ratio7': 'Net Profit Margin',
    'ratio8': 'Asset Turnover',
    'ratio9': 'Inventory Turnover',
    'ratio10': 'Receivable Turnover',
    'ratio11': 'Days Sales In Receivables',
    'ratio12': 'ROE',
    'ratio13': 'ROTE',
    'ratio14': 'ROA ',
    'ratio15': 'ROI',
    'ratio16': 'Book Value Per Share',
    'ratio17': 'Operating Cash Flow Per Share',
    'ratio18': 'Free Case Flow Per Share',
    'Stock_Return': 'Stock Return'
}

fieldnames = [
 'CO_ID',
 'Name',
 'Year',
 'Quarter',
 'Sector',
 'Current Ratio',
 'Long Term Debt / Captial',
 'Debt / Equity',
 'Gross Margin',
 'Operating Margin',
 'Pre-Tax Profit Margin',
 'Net Profit Margin',
 'Asset Turnover',
 'Inventory Turnover',
 'Receivable Turnover',
 'Days Sales In Receivables',
 'ROE',
 'ROTE',
 'ROA ',
 'ROI',
 'Book Value Per Share',
 'Operating Cash Flow Per Share',
 'Free Case Flow Per Share',
 'Stock Return',
 'Relative Return'
 ]


Q1_START = ('-05-20', '-05-25')
Q1_END = ('-08-19', '-08-24')
Q2_START = ('-08-20', '-08-25')
Q2_END = ('-11-19', '-11-24')
Q3_START = ('-11-20', '-11-25')
Q3_END = ('-02-19', '-02-27')
Q4_START = ('-02-20', '-02-27')
Q4_END = ('-05-19', '-05-24')


def _stock_price_helper(id, Y, date_suffix):
    '''
        收盤價 Close
    '''
    df = yf.download(id, start=Y+date_suffix[0], end=Y+date_suffix[-1])
    return df['Close'][0] # take the first available row

def stock_return_parser(param):
    id, Year, Quarter = param
    id += '.TW'

    print(f'\nCalculating Stock Return of {id}, {Year}, {Quarter}')

    growth_rate = lambda start, end: (end - start)/start
    incr = lambda Y: str(int(Year) + 1) # year increment in string

    if Quarter == '1':
        start = _stock_price_helper(id, Year, Q1_START)
        end = _stock_price_helper(id, Year, Q1_END)
    elif Quarter == '2':
        start = _stock_price_helper(id, Year, Q2_START)
        end = _stock_price_helper(id, Year, Q2_END)
    elif Quarter == '3':
        start = _stock_price_helper(id, Year, Q3_START)
        end = _stock_price_helper(id, incr(Year), Q3_END)
    elif Quarter == '4':
        start = _stock_price_helper(id, incr(Year), Q4_START)
        end = _stock_price_helper(id, incr(Year), Q4_END)
    else:
        raise Exception(f"[ERROR] illegal Quarter: {Quarter}")
    # 絕對stock return
    result = growth_rate(start, end)

    return result

def fetch_entry(CO_ID, Year, Quarter):
    url = "http://127.0.0.1:8000"   # localhost
    url += f'/company_data/{CO_ID}/final_data?Year={Year}&Quarter={Quarter}'
    response = requests.get(url)
    entry = response.json()

    if entry['data_exist']:
        del entry['data_exist']

        data = dict()
        data['Name'] = INPUT_DF[INPUT_DF['CO_ID'] == CO_ID]['CO_NAME'].values[0]

        for x in entry:
            # set INF -> None
            if entry[x] == 100000000:
                entry[x] = None
                
            data[title[x]] = entry[x]

        # calculate Relative return
        # 相對return = 個股 - 大盤
        data['Relative Return'] = data['Stock Return'] - base_return[(Year, Quarter)]

        return data
    else:
        print(f'[WARNING] entry not exist: {CO_ID}, {Year}, {Quarter}')
        return None



############## execution start HERE ##############

# collect all base return
Y = int(start_Year)
Q = int(start_Quarter)
while 1:
    base_return[(Y, Q)] = stock_return_parser((BASE_ID, str(Y), str(Q)))

    if (Y, Q) == (int(end_Year), int(end_Quarter)):
        break

    Y += (Q == 4)
    Q = (Q%4) + 1


# generate ratio.csv
with open(PATH_TO_CSV, 'w') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()    # write first row (column names)
    
    CO_ID_list = INPUT_DF['CO_ID'].values
    for id in CO_ID_list:
        print(f'add company: {id}')
        Y = int(start_Year)
        Q = int(start_Quarter)
        while 1:
            row = fetch_entry(id, Y, Q)

            if row is not None:
                writer.writerow(row)

            if (Y, Q) == (int(end_Year), int(end_Quarter)):
                break

            Y += (Q == 4)
            Q = (Q%4) + 1

    # 大盤資訊
    # for year in range(2013, 2023):
    #     for quarter in range(1,5):
    #         if year == 2022 and quarter == 2:
    #             break

    #         row = {
    #             'CO_ID': BASE_ID + '.TW',
    #             'Year': str(year),
    #             'Quarter': str(quarter),
    #             'Stock Return': base_return[(year, quarter)]
    #         }
    #         writer.writerow(row)