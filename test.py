from  fastapi import FastAPI, Query
from typing import List
from datetime import date

import pandas as pd
import urllib
from config import INSTANCE, DB, get_db_data,  upsert_db_data, get_db_sqlite_data, upsert_db_sqlite_data
import pyodbc
import json

# from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import datetime
import time
import sys
from models import Audit
import queries_sqlite
from  fastapi import FastAPI, Query, Response
from models import RefAc


# timestamp = 1676341296420
# dt_object = datetime.fromtimestamp(timestamp)

# print("dt_object =", dt_object)
# print("type(dt_object) =", type(dt_object))


# audit_date = '2023-02-14'
# sql = sql_GetAuditResults.format(audit_date)
# df = get_db_data(INSTANCE, DB, sql)
# print(df)

audit_date = '2023-02-14'
sql = queries_sqlite.sql_RefAc.format(audit_date)
df = get_db_sqlite_data('hbiapi.sqlite', sql)
print(df.head)
# print(df.to_json(orient="records"))


sql = queries_sqlite.sql_insert_RefAc.format('test2', '2023-02-01', '2023-02-01', 0.43)
res = upsert_db_sqlite_data('hbiapi.sqlite', sql)
print(res.rowcount)

audit_date = '2023-02-14'
sql = queries_sqlite.sql_RefAc.format(audit_date)
df = get_db_sqlite_data('hbiapi.sqlite', sql)
print(df.head)
# print(df.to_json(orient="records"))


# unix_ts = 1676341351250
# timestamp = "1676341351250"
# your_dt = datetime.datetime.fromtimestamp(int(timestamp)/1000) - datetime.timedelta(hours=3)  # using the local timezone
# print(your_dt)
# print(your_dt.strftime("%Y-%m-%d %H:%M:%S"))

