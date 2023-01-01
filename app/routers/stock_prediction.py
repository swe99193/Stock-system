from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException
from ..dependencies import get_db
from .. import crud_pred, models, schemas

from datetime import date
import os
import pickle
import pandas as pd

router = APIRouter(prefix="/stock_prediction")

PATH_pred = os.path.join("stock-selection-system", "predictions")

INPUT_DF = pd.read_csv(os.path.join('csv', 'target_company.csv')) # target company ids


# FIXME: data and result stop at 2022 Q1
# When automation of data parser & model training is available, use current date instead 
today = date(2022, 6, 13)

def date_to_quarter(month, day):
    ''' according to our definition of each season's timestamps '''
    if month < 2 and day < 26:
        quarter = 3
    elif month < 5 and day < 26:
        quarter = 4
    elif month < 8 and day < 26:
        quarter = 1
    elif month < 11 and day < 26:
        quarter = 2
    else:
        quarter = 3

    return quarter

# rank all companies in a quarter
@router.get("/rank")
async def get_company_rank(
    Model: str
    ):
    # today = date.today()
    Year, month, day = today.strftime("%Y/%m/%d").split('/')
    Quarter = date_to_quarter(int(month), int(day))

    query_param = dict()
    query_param['Year'] = int(Year)
    query_param['Quarter'] = Quarter
    query_param['Model'] = Model

    try:
        fp = open(os.path.join(PATH_pred, Model, f"{Year}-{Quarter}-company-rank.pickle"), "rb")
        co_id_rank = pickle.load(fp)
        fp.close()
    except:
        raise HTTPException(status_code=404, detail="Item not found")
    
    for i, co_id in enumerate(co_id_rank):
        co_id_rank[i] = f"{co_id}    {INPUT_DF[INPUT_DF['CO_ID'] == co_id]['CO_NAME'].values[0]}"

    return {
        'Year': int(Year),
        'Quarter': Quarter,
        'Model': Model,
        'co_id_rank': co_id_rank
    }

# retrieve the latest 5 quarters (count from current season)
@router.get("/individual_return")
async def get_seasonal_return(
    Model: str,
    Strategy: str,
    db: Session = Depends(get_db)
    ):
    # today = date.today()
    Year, month, day = today.strftime("%Y/%m/%d").split('/')
    Quarter = date_to_quarter(int(month), int(day))

    query_param = dict()
    query_param['Year'] = int(Year)
    query_param['Quarter'] = Quarter
    query_param['Model'] = Model
    query_param['Strategy'] = Strategy
    
    result = crud_pred.get_seasonal_return(db, query_param)

    if result:  # not empty
        return result
    else:
        raise HTTPException(status_code=404, detail="Item not found")

# calculate accumulated return from the given time stamp to now
@router.get("/accumulated_return")
async def get_accumulated_return(
    Year: int,
    Quarter: int,
    Model: str,
    Strategy: str,
    db: Session = Depends(get_db)
    ):
    query_param = dict()
    query_param['Year'] = Year
    query_param['Quarter'] = Quarter
    query_param['Model'] = Model
    query_param['Strategy'] = Strategy
    
    result = crud_pred.get_accumulated_return(db, query_param)

    if result:  # not empty
        return result
    else:
        raise HTTPException(status_code=404, detail="Item not found")

