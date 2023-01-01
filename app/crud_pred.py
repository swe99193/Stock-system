from sqlalchemy.orm import Session
import copy
import pandas as pd
import os

from . import models, schemas


def get_seasonal_return(db: Session, query_param: dict):
    s_Year = query_param['Year'] - 1
    result = db.query(models.Return_equal_weight).filter(
        models.Return_equal_weight.Model == query_param['Model'],
        models.Return_equal_weight.Strategy == query_param['Strategy'],
        models.Return_equal_weight.Year >= s_Year
        ).all()

    result = [r.__dict__ for r in result]
    result.sort(key=lambda x: (x['Year'], x['Quarter']))    # order by time
    
    return result[-5:]  # latest 5 entries

def get_accumulated_return(db: Session, query_param: dict):
    result = db.query(models.Return_equal_weight).filter(
        models.Return_equal_weight.Model == query_param['Model'],
        models.Return_equal_weight.Strategy == query_param['Strategy'],
        models.Return_equal_weight.Year >= query_param['Year'],
        ).all()
    
    result = [r.__dict__ for r in result]
    result.sort(key=lambda x: (x['Year'], x['Quarter']))    # order by time

    trunc = 0
    for res in result:
        if res['Year'] == query_param['Year'] and res['Quarter'] == query_param['Quarter']:
            break
        else:
            trunc += 1

    result = result[trunc:]

    # calculate accumulated value
    if result:
        result[0]['Stock_Return'] += 1   # 0.05 -> 1.05
        for i in range(1, len(result)):
            result[i]['Stock_Return'] += 1
            result[i]['Stock_Return'] *= result[i-1]['Stock_Return']

        prev_year = result[0]['Year']
        prev_quarter = result[0]['Quarter']
        head = {
            'Year': prev_year - (prev_quarter == 1),
            'Quarter': (prev_quarter - 2)%4 + 1,
            'Model': query_param['Model'],
            'Strategy': query_param['Strategy'],
            'Stock_Return': 1,
        }
        result = [head] + result

    return result
