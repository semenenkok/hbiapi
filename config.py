from typing import List
from datetime import date

from sqlalchemy import create_engine, text
import pandas as pd
import urllib

INSTANCE='WIN-AEBL18K52MS\SQLEXPRESS'
DB='HDWH'

def set_conn(instance, db):
    try:
        conStr = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};DATABASE={1};Trusted_Connection=yes'.format(instance, db)
        params = urllib.parse.quote_plus(conStr)
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, isolation_level="AUTOCOMMIT")
        return engine
    except Exception as e:
        print(str(e))
        raise

def get_db_data(instance, db, sql):
    engine = set_conn(instance, db)
    return pd.DataFrame(engine.connect().execute(text(sql)))


def get_db_data2(instance, db, sql):
    engine = set_conn(instance, db)
    conn = engine.connect()
    conn.execution_options='AUTOCOMMIT'
    return pd.DataFrame(conn.execute(text(sql)))


def upsert_db_data(instance, db, sql):
    engine = set_conn(instance, db)
    conn = engine.connect()
    conn.execution_options='AUTOCOMMIT'
    return conn.execute(text(sql))
