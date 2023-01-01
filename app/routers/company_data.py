from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends
from ..dependencies import get_db
from .. import crud, models, schemas

router = APIRouter(prefix="/company_data", tags=['company_data'])


# company data: financial ratios, stock return
@router.get("/{CO_ID}/final_data", response_model=schemas.FinalData)
async def get_company_final_data(
    CO_ID: str, # path parameters
    Year: int,
    Quarter: int,
    db: Session = Depends(get_db)
    ):
    query_param = dict()
    query_param['CO_ID'] = CO_ID
    query_param['Year'] = Year
    query_param['Quarter'] = Quarter
    return crud.get_company_final_data(db, query_param)


# company data: financial ratios, stock return
@router.get("/{CO_ID}/latest_final_data")
async def get_company_final_data(
    CO_ID: str, # path parameters
    Year: int,
    Quarter: int,
    db: Session = Depends(get_db)
    ):
    query_param = dict()
    query_param['CO_ID'] = CO_ID
    query_param['Year'] = Year
    query_param['Quarter'] = Quarter
    return crud.get_company_latest_final_data(db, query_param)
