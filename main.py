from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Query, Response
from typing import List, Optional
from datetime import date

#import auth.database  as auth 


from sqlalchemy import create_engine, text
import pandas as pd
import urllib
from config import INSTANCE, DB, get_db_data, upsert_db_data, get_db_sqlite_data, upsert_db_sqlite_data
import json
import queries
import queries_sqlite
from models import RefAc, RefAc_ins, RefAc_del, RefVat, RefVat_del

app = FastAPI()
#auth.create_db_and_tables()


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

#######RefAc######
@app.get('/RefAc', description='select data from dv.RefAc')
def get_RefAc():
    df = get_db_sqlite_data('hbiapi.sqlite', queries_sqlite.sql_RefAc)
    return Response(df.to_json(orient="records"), media_type="application/json")


@app.post('/RefAc_ins', description='insert data into RefAc')
def insert_RefAc(RefAc: RefAc_ins):
    sql = queries_sqlite.sql_insert_RefAc.format(RefAc.BK_SourceMediumCode, RefAc.startDate, RefAc.endDate, RefAc.acRate, RefAc.acRate)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefAc_upd', description='update data into RefAc')
def update_RefAc(RefAc: RefAc):
    sql = queries_sqlite.sql_update_RefAc.format(RefAc.id, RefAc.BK_SourceMediumCode, RefAc.startDate, RefAc.endDate, RefAc.acRate, RefAc.acRate)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefAc_del', description='delete data from RefAc by id')
def delete_RefAc(RefAc_del: RefAc_del):
    sql = queries_sqlite.sql_delete_RefAc.format(RefAc_del.id)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

#######RefVat######
@app.get('/RefVat', description='select data from dv.RefVat')
def get_RefVat():
    df = get_db_sqlite_data('hbiapi.sqlite', queries_sqlite.sql_RefVat)
    return Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefVat', description='upsert data into RefVat (upsert - insert news and update existence by id)')
def upsert_RefVat(RefVat: RefVat):
    sql = queries_sqlite.sql_upsert_RefVat.format(RefVat.id, RefVat.startDate, RefVat.endDate, RefVat.vatRate, RefVat.vatRate)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefVat_del', description='delete data from RefVat by id')
def delete_RefVat(RefVat_del: RefVat_del):
    sql = queries_sqlite.sql_delete_RefVat.format(RefVat_del.id)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

#######RefVatArm######
@app.get('/RefVatArm', description='select data from dv.RefVatArm')
def get_RefVatArm():
    df = get_db_sqlite_data('hbiapi.sqlite', queries_sqlite.sql_RefVatArm)
    return Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefVatArm', description='upsert data into RefVatArm (upsert - insert news and update existence by id)')
def upsert_RefVatArm(RefVat: RefVat):
    sql = queries_sqlite.sql_upsert_RefVatArm.format(RefVat.id, RefVat.startDate, RefVat.endDate, RefVat.vatRate, RefVat.vatRate)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")

@app.post('/RefVatArm_del', description='delete data from RefVatArm by id')
def delete_RefVatArm(RefVat_del: RefVat_del):
    sql = queries_sqlite.sql_delete_RefVatArm.format(RefVat_del.id)
    res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
    return res.rowcount 
    #Response(df.to_json(orient="records"), media_type="application/json")


# @app.get('/book')
# def get_book(q: List[str] = Query('defaultbook name', description='Search book')):
#     return q

