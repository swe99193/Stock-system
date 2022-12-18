from sqlalchemy.orm import Session
import copy
import pandas as pd
import os

from . import models, schemas


INPUT_DF = pd.read_csv(os.path.join('csv', 'target_company.csv')) # target company ids


response_format = {
    "CO_NAME": None,
    "CO_ID": None,
    "Year": None,
    "Quarter": None,
    "ratio1": [None, None, None, None, None],
    "ratio2": [None, None, None, None, None],
    "ratio3": [None, None, None, None, None],
    "ratio4": [None, None, None, None, None],
    "ratio5": [None, None, None, None, None],
    "ratio6": [None, None, None, None, None],
    "ratio7": [None, None, None, None, None],
    "ratio8": [None, None, None, None, None],
    "ratio9": [None, None, None, None, None],
    "ratio10": [None, None, None, None, None],
    "ratio11": [None, None, None, None, None],
    "ratio12": [None, None, None, None, None],
    "ratio13": [None, None, None, None, None],
    "ratio14": [None, None, None, None, None],
    "ratio15": [None, None, None, None, None],
    "ratio16": [None, None, None, None, None],
    "ratio17": [None, None, None, None, None],
    "ratio18": [None, None, None, None, None],
    "Stock_Return": [None, None, None, None, None]
}

colName_map = {
    'ratio1': 'Current Ratio',
    'ratio2': 'Long Term Debt to Capital Ratio',
    'ratio3': 'Debt to Equity Ratio',
    'ratio4': 'Gross Margin',
    'ratio5': 'Operating Margin',
    'ratio6': 'Pre-Tax Profit Margin',
    'ratio7': 'Net Profit Margin',
    'ratio8': 'Asset Turnover',
    'ratio9': 'Inventory Turnover Ratio',
    'ratio10': 'Receivable Turnover',
    'ratio11': 'Days Sales In Receivables ',
    'ratio12': 'ROE',
    'ratio13': 'ROTE',
    'ratio14': 'ROA ',
    'ratio15': 'ROI',
    'ratio16': 'Book Value Per Share',
    'ratio17': 'Operating Cash Flow Per Share',
    'ratio18': 'Free Case Flow Per Share',
    'Stock_Return': 'Stock Return'
}

def get_company_final_data(db: Session, query_param: dict):
    result = db.query(models.Final_data).filter(
        models.Final_data.CO_ID == query_param['CO_ID'],
        models.Final_data.Year == query_param['Year'],
        models.Final_data.Quarter == query_param['Quarter']
        ).first()
    
    if result:
        final_result = result.__dict__
        final_result['data_exist'] = True
    else:
        final_result = {'data_exist': False}
        
    return final_result


def get_company_latest_final_data(db: Session, query_param: dict):
    N = 5   # the previous 5 quarters
    result = db.query(models.Final_data).filter(
        models.Final_data.CO_ID == query_param['CO_ID']
        ).all()

    result = [r.__dict__ for r in result]
    result.sort(key=lambda x: (x['Year'], x['Quarter']))    # order by time

    CO_ID = query_param['CO_ID']
    YEAR = s_year = e_year = query_param['Year']
    QUARTER = s_quarter = e_quarter = query_param['Quarter']

    final_result = copy.deepcopy(response_format)


    if len(INPUT_DF[INPUT_DF['CO_ID'] == CO_ID]) == 0:
        return

    final_result['CO_NAME'] = INPUT_DF[INPUT_DF['CO_ID'] == CO_ID]['CO_NAME'].values[0]
    final_result['CO_ID'] = CO_ID
    final_result['Year'] = YEAR
    final_result['Quarter'] = QUARTER

    if result and final_result['CO_NAME']:
        # retrieve the previous 5 quarters data given the year and quarter

        # # calculate start year and quarter
        # for i in range(N-1):
        #     if s_quarter == 1:
        #         s_quarter += 4
        #         s_year -= 1
        #     s_quarter -= 1

        targetYQ = [None for _ in range(N)] # [past -> now]
        Y, Q = YEAR, QUARTER
        for i in range(N-1, -1, -1):
            targetYQ[i] = Y, Q

            Y -= (Q == 1)
            Q = ((Q-2)%4) + 1

        # append ratios
        for i, t in enumerate(targetYQ):
            for res in result:
                if t == (res['Year'], res['Quarter']):
                    for key in colName_map:
                        final_result[key][i] = res[key]
        
    return final_result
