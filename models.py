from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional
from sqlalchemy import MetaData, Table, column

class Audit(BaseModel):
    ExecId: int 


class RefAc(BaseModel):
    id: int 
    BK_SourceMediumCode: str
    startDate: datetime 
    endDate: datetime 
    acRate: float

class RefAc_ins(BaseModel):
    BK_SourceMediumCode: str
    startDate: datetime 
    endDate: datetime 
    acRate: float

class RefAc_del(BaseModel):
    id: int

class RefVat(BaseModel):
    id: int
    startDate: datetime 
    endDate: datetime 
    vatRate: float

class RefVat_del(BaseModel):
    id: int


