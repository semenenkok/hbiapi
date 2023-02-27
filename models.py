from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional

class Audit(BaseModel):
    ExecId: int 


class RefAc(BaseModel):
    BK_SourceMediumCode: str
    startDate: datetime 
    endDate: datetime 
    acRate: float

class RefAc_del(BaseModel):
    BK_SourceMediumCode: str
    startDate: datetime 
    endDate: datetime 

class RefVat(BaseModel):
    startDate: datetime 
    endDate: datetime 
    vatRate: float

class RefVat_del(BaseModel):
    startDate: datetime 
    endDate: datetime 

