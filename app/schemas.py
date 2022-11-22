from pydantic import BaseModel
from typing import List, Union, Optional

class FinalDataBase(BaseModel):
    data_exist: bool

    CO_ID: Optional[str]
    Year: Optional[int]
    Quarter: Optional[int]
    Sector: Optional[str]

    ratio1: Optional[float]
    ratio2: Optional[float]
    ratio3: Optional[float]
    ratio4: Optional[float]
    ratio5: Optional[float]
    ratio6: Optional[float]
    ratio7: Optional[float]
    ratio8: Optional[float]
    ratio9: Optional[float]
    ratio10: Optional[float]
    ratio11: Optional[float]
    ratio12: Optional[float]
    ratio13: Optional[float]
    ratio14: Optional[float]
    ratio15: Optional[float]
    ratio16: Optional[float]
    ratio17: Optional[float]
    ratio18: Optional[float]

    Stock_Return: Optional[float]

class FinalData(FinalDataBase):
    # email: str
    
    class Config:
        orm_mode = True