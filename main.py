from fastapi.middleware.cors import CORSMiddleware
from  fastapi import FastAPI, Query, Response
from typing import List, Optional
from datetime import date

from sqlalchemy import create_engine, text
import pandas as pd
import urllib
from config import INSTANCE, DB, get_db_data, upsert_db_data, get_db_sqlite_data, upsert_db_sqlite_data
import json
import queries
import queries_sqlite
from models import RefAc

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/')
def home():
    return {'key': 'Hello Henderson BI Studio API'}


@app.get('/audit/')
def get_user_item(audit_date: date = Query(..., description='Audit date')):
    sql = queries_sqlite.sql_GetAuditResults #.format(audit_date)
    df = get_db_sqlite_data('hbiapi.sqlite', sql)
    return Response(df.to_json(orient="records"), media_type="application/json")
    # return Response(df.to_json(orient="records"), media_type="application/json")


@app.get('/RefAc', description='select data from dv.RefAc')
def get_RefAc():
    df = get_db_sqlite_data('hbiapi.sqlite', queries_sqlite.sql_RefAc)
    return Response(df.to_json(orient="records"), media_type="application/json")


@app.post('/RefAc', description='insert data into RefAc')
def create_RefAc(RefAc: RefAc):
    sql = queries_sqlite.sql_insert_RefAc.format(RefAc.BK_SourceMediumCode, RefAc.startDate, RefAc.endDate, RefAc.acRate)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

# @app.get('/book')
# def get_book(q: List[str] = Query('defaultbook name', description='Search book')):
#     return q

