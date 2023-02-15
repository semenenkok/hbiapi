from  fastapi import FastAPI, Query, Response
# from schemas import Book
from typing import List, Optional
from datetime import date

from sqlalchemy import create_engine, text
import pandas as pd
import urllib
from config import INSTANCE, DB, get_db_data, upsert_db_data
import json
from queries import sql_GetAuditResults, sql_RefAc
from models import RefAc

app = FastAPI()


@app.get('/')
def home():
    return {'key': 'Hello Henderson BI Studio API'}


@app.get('/audit/')
def get_user_item(audit_date: date = Query(..., description='День аудита')):
    sql = sql_GetAuditResults.format(audit_date)
    df = get_db_data(INSTANCE, DB, sql)
    return Response(df.to_json(orient="records"), media_type="application/json")
    # return Response(df.to_json(orient="records"), media_type="application/json")


@app.get('/RefAc', description='select data from dv.RefAc')
def get_RefAc():
    df = get_db_data(INSTANCE, DB, sql_RefAc)
    return Response(df.to_json(orient="records"), media_type="application/json")


@app.post('/RefAc', description='insert data into dv.RefAc')
def create_RefAc(RefAc: RefAc):
    sql = """INSERT INTO bv.RefAc (BK_SourceMediumCode, startDate, endDate, acRate)
             VALUES ('{0}','{1}','{2}',{3})
          """.format(RefAc.BK_SourceMediumCode, RefAc.startDate, RefAc.endDate, RefAc.acRate)
    res = upsert_db_data(INSTANCE, DB, sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

# @app.get('/book')
# def get_book(q: List[str] = Query('defaultbook name', description='Search book')):
#     return q

