from pydantic import BaseModel
from datetime import date

class Audit(BaseModel):
    ExecId: int 


class RefAc(BaseModel):
    BK_SourceMediumCode: str
    startDate: date 
    endDate: date
    acRate: float
