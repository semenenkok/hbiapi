import pyodbc
import SQLQueries #import parameters from separated file
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import event
import urllib
import pandas as pd
from datetime import datetime
import time
import sys

class StageLoader:

    def __init__(self, hub_params):
        self.env = hub_params.env    
        self.obj_group = hub_params.obj_group    
        self.target_schema = hub_params.target_schema      
        self.target_name = hub_params.target_name
        self.stg_schema = hub_params.stg_schema          
        self.isEnabled = hub_params.isEnabled            
        self.s_server = hub_params.s_server            
        self.s_db = hub_params.s_db                
        self.s_Trusted_Connection = hub_params.s_Trusted_Connection
        self.s_username = hub_params.s_username          
        self.s_password = hub_params.s_password          
        self.s_sql_sel = hub_params.s_sql_sel            
        self.t_server = hub_params.t_server            
        self.t_db = hub_params.t_db                
        self.t_Trusted_Connection = hub_params.t_Trusted_Connection
        self.t_username = hub_params.t_username          
        self.t_password = hub_params.t_password          
        self.t_sql_ins = hub_params.t_sql_ins
        self.t_timestamp_field_name = hub_params.t_timestamp_field_name
        self.t_timestamp_field_type = hub_params.t_timestamp_field_type
        self.batch_size = hub_params.batch_size    
        self.unknownBatchSize = hub_params.unknownBatchSize
        self.sp_to_send_dv = hub_params.sp_to_send_dv        
        self.ds_name_source = hub_params.ds_name_source
        self.ds_name_target = hub_params.ds_name_target
        self.s_type = hub_params.s_type  
       
       
        self.Execid = -1
       
        #Create logger of process
        self.logger = logging.getLogger("MainStageLoaderLogger")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler("StageLogger.log")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        #Соединение только для источников типа DB
        if self.s_type.upper() == 'DB':
            self.Set_source_Conn()
            self.Set_s_engine()
       
        self.Set_target_Conn()
        self.Set_t_engine()
       
       

    #декоратор для логирования
    def getLogging(func):
        def wrapper(*args, **kwargs):
            start = datetime.now()
            print(str(start.strftime("%H:%M:%S")) + ' Start ' + func.__name__)
            result = func(*args, **kwargs)
            finish = datetime.now()
            print(str(finish.strftime("%H:%M:%S")) + ' End   ' + func.__name__ + '. Result is: ' + str(result) + ' rows. Duration is: ' + str(finish - start))
            return result
        return wrapper
           
    @staticmethod
    def GetConn(server, db, Trusted_Connection):
        server = server
        db = db
        username = '<username>'
        password = '<password>'
        driver = '{ODBC Driver 17 for SQL Server}'
        if Trusted_Connection:
            connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};Trusted_Connection=yes;'.format(driver, server, db)
        else:
            connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(driver, server, db, username , password)
        conn = pyodbc.connect(connSrt)
        conn.autocommit = True
        return conn

    #Get hub to load by hubname
    @staticmethod
    def GethubToLoadbyHubname(conn, env, hub_group):
            try:
                cursor = conn.cursor()
                sql = SQLQueries.GethubToLoadbyHubname
                params = (env,env,env, hub_group)
                cursor.execute(sql, params)
                hub_params = cursor.fetchall()
                return hub_params
            except Exception as e:
               cursor.cancel()
               raise

       

    def Set_s_engine(self):
        try:
            if self.s_Trusted_Connection:
                conStr = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};DATABASE={1};Trusted_Connection=yes'.format(self.s_server, self.s_db)
            else:
                conStr = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};DATABASE={1};UID={2};PWD={3}'.format(self.s_server, self.s_db, self.s_username , self.s_password)
            params = urllib.parse.quote_plus(conStr)
            self.s_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        except Exception as e:
            self.WriteErrorMessage(str(e), self.Execid)
            raise
     
    def Set_t_engine(self):
        try:
            conStr = 'DRIVER=ODBC Driver 17 for SQL Server;SERVER={0};DATABASE={1};Trusted_Connection=yes'.format(self.t_server, self.t_db)
            params = urllib.parse.quote_plus(conStr)
            self.t_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        except Exception as e:
            self.WriteErrorMessage(str(e), self.Execid)
            raise


    #Set souruce conn
    def Set_source_Conn(self):
        try:
            self.s_driver = '{ODBC Driver 17 for SQL Server}'
            if self.s_Trusted_Connection:
                self.s_connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};Trusted_Connection=yes;'.format(self.s_driver, self.s_server, self.s_db)
            else:
                self.s_connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(self.s_driver, self.s_server, self.s_db, self.s_username , self.s_password)
            self.s_conn = pyodbc.connect(self.s_connSrt)
            self.s_conn.autocommit = True
        except Exception as e:
            self.WriteErrorMessage(str(e), self.Execid)
            raise


    #Set target conn
    def Set_target_Conn(self):
        try:
            self.t_driver = '{ODBC Driver 17 for SQL Server}'
            if self.t_Trusted_Connection:
                self.t_connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};Trusted_Connection=yes;'.format(self.t_driver, self.t_server, self.t_db)
            else:
                self.t_connSrt = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'.format(self.t_driver, self.t_server, self.t_db, self.t_username , self.t_password)
            self.t_conn = pyodbc.connect(self.t_connSrt)
            self.t_conn.autocommit = True
        except Exception as e:
            self.WriteErrorMessage(str(e), self.Execid)
            raise


    #Регистрация начала работы процесса в таблице AuditProcessExecution
    def AuditProcessExecution_Insert(self, type, ParentExecId, Name, UserName, ExecStartDate):
        try:
            t_cursor = self.t_conn.cursor()

            #По схеме, куда загружаем данные, определяем аудит это или нет, если аудит, то лог будет писаться в таблицу AuditProcessExecution_audit
            if self.target_schema == 'aud':
                params = (type, ParentExecId, Name, UserName, ExecStartDate)
                query =  SQLQueries.AuditProcessExecution_Insert_audit
            else:
                params = (type, ParentExecId, Name, UserName, ExecStartDate)
                query = SQLQueries.AuditProcessExecution_Insert

            t_cursor.execute(query, params)
            Execid = t_cursor.fetchval()
            self.Execid = Execid

        except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise

    #Регистрация окончания работы/падения процесса в таблице AuditProcessExecution
    def AuditProcessExecution_ExecStopDate_Update(self, Completed, ExecStopDate):
            try:
                t_cursor = self.t_conn.cursor()

                 #По схеме, куда загружаем данные, определяем аудит это или нет, если аудит, то лог будет писаться в таблицу AuditProcessExecution_audit
                if self.target_schema == 'aud':
                    param = (Completed, ExecStopDate, self.Execid)
                    query = SQLQueries.AuditProcessExecution_ExecStopDate_audit_Update
                else:
                    param = (Completed, ExecStopDate, self.Execid)
                    query = SQLQueries.AuditProcessExecution_ExecStopDate_Update

                t_cursor.execute(query, param)

            except Exception as e:
                t_cursor.cancel()
                self.WriteErrorMessage(str(e), self.Execid)
                raise

    #GetLatestTimeStamp
    def GetLatestTimeStamp(self):
           try:
               ts = ''
               ts_binary = None
               ts_date = None
               ts_int = None
               
               sql = ''
               if self.t_timestamp_field_type == 'binary(8)':
                   sql = SQLQueries.GetLatestTimeStamp
               elif self.t_timestamp_field_type == 'datetime':
                   sql = SQLQueries.GetLatestTimeStamp_asDate
               elif self.t_timestamp_field_type == 'int':
                   sql = SQLQueries.GetLatestTimeStamp_asInt


               if sql != '':
                   t_cursor = self.t_conn.cursor()
                   params = (self.target_schema, self.target_name, self.ds_name_source, self.obj_group )
                   t_cursor.execute(sql, params)
                   ts = t_cursor.fetchval()
                     


               if self.t_timestamp_field_type == 'binary(8)':
                   ts_binary = ts
               elif self.t_timestamp_field_type == 'datetime':
                   ts_date = ts
               elif self.t_timestamp_field_type == 'int':
                   ts_int = ts

               ts_lst = [ts, ts_binary, ts_date, ts_int]
               
               return ts_lst
           except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise
               

    #GetMaxLoadedTimeStamp
    def GetMaxLoadedTimeStamp(self):
            try:
                if self.t_timestamp_field_name != '':
                    t_cursor = self.t_conn.cursor()
                    sql = SQLQueries.GetMaxTimeStamp.format(self.stg_schema, self.target_name, self.t_timestamp_field_name)
                    t_cursor.execute(sql)
                    maxTs = t_cursor.fetchval()
                else:
                    maxTs = None
                return maxTs
            except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise


    #GetMaxLoadedTimeStamp
    def SetLatestTimeStamp(self, timestamp):
            try:
                tsmax_binary = None
                tsmax_date = None
                tsmax_int = None

                sql = ''
                if timestamp is not None:
                    if 'date' in type(timestamp).__name__ :
                        sql = SQLQueries.SetLatestTimeStamp_asDate
                        tsmax_date = timestamp

                    if (type(timestamp).__name__ == 'bytes'):
                        sql = SQLQueries.SetLatestTimeStamp
                        tsmax_binary = timestamp

                    if (type(timestamp).__name__ == 'int'):
                        sql = SQLQueries.SetLatestTimeStamp_asInt
                        tsmax_int = timestamp


                if sql != '':
                    t_cursor = self.t_conn.cursor()
                    param = (self.target_schema, self.target_name , self.ds_name_source, self.obj_group, timestamp)
                    t_cursor.execute(sql, param)

                ts_lst = [tsmax_binary, tsmax_date, tsmax_int]

                return ts_lst
            except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise


    def TruncateStage(self):
        try:
            t_truncate_query = SQLQueries.trun_from_source_query.format(self.stg_schema, self.target_name)
            t_cursor = self.t_conn.cursor()
            t_cursor.execute(t_truncate_query)
        except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise


    ##Метод загрузки данных из источника в stage батчами (размер батча batchsize)
    #def ImportDataByBatch(self, ts):
    #    try:
    #        #t_truncate_query =
    #        SQLQueries.trun_from_source_query.format(self.stg_schema,
    #        self.target_name)
    #        t_cursor = self.t_conn.cursor()
    #        #t_cursor.execute(t_truncate_query)
           
    #        s_cursor = self.s_conn.cursor()
           
    #        if ts != '':
    #            s_cursor.execute(self.s_sql_sel, ts) #parametrize query
    #        else:
    #            s_cursor.execute(self.s_sql_sel)

    #        #print('Start batch processing, batchsize is: ' +
    #        str(self.batch_size))
    #        rows = s_cursor.fetchmany(self.batch_size)
    #        while len(rows) != 0:
    #            rowcnt = len(rows)
    #            print('batch ' + str(rowcnt) + ' process..')
    #            t_cursor.fast_executemany = True
    #            t_cursor.executemany(self.t_sql_ins, rows)
    #            rows = s_cursor.fetchmany(self.batch_size)
    #    except Exception as e:
    #           t_cursor.cancel()
    #           self.WriteErrorMessage(str(e), self.Execid)
    #           raise

    #Метод загрузки данных из источника в stage батчами (размер батча batchsize)
    def ImportDataByBatch2(self, ts):
        try:
            t_cursor = self.t_conn.cursor()
            s_cursor = self.s_conn.cursor()
           
            if ts != '':
                s_cursor.execute(self.s_sql_sel, ts) #parametrize query
            else:
                s_cursor.execute(self.s_sql_sel)

            loadedRowsCnt = 0
            rows = s_cursor.fetchmany(self.batch_size)
            loadedRowsCnt += len(rows)
            while len(rows) != 0:
                t_cursor.fast_executemany = True
                t_cursor.executemany(self.t_sql_ins, rows)
                rows = s_cursor.fetchmany(self.batch_size)
                loadedRowsCnt += len(rows)

            return loadedRowsCnt
        except Exception as e:
               t_cursor.cancel()
               self.WriteErrorMessage(str(e), self.Execid)
               raise


    #По сравнению с ImportDataByBatch2 этот метод загрузки данных из источника в stage батчами гораздо более быстрый
    @getLogging
    def ImportDataByBatchFast(self, ts):
        try:
            rowCnt = 0
            if self.s_sql_sel != '':
                if ts != '':
                    prm = [ts]
                    df = pd.read_sql_query(self.s_sql_sel, self.s_engine, params = prm)
                else:
                    df = pd.read_sql_query(self.s_sql_sel, self.s_engine)

                @event.listens_for(self.t_engine, "before_cursor_execute")
                def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

                df.to_sql(schema = self.stg_schema, name = self.target_name , con = self.t_engine, if_exists='append', index = False, chunksize=self.batch_size)
                rowCnt = len(df)
            return rowCnt
        except Exception as e:
               self.WriteErrorMessage(str(e), self.Execid)
               raise


    #WriteErrorMessage
    def WriteErrorMessage(self, ex, ExecId):
        t_cursor = self.t_conn.cursor()
        param = (str(ex), ExecId, 'Py')
        t_cursor.execute(SQLQueries.WriteErrorMessage, param)


    #WriteErrorMessageNew НЕ ИСПОЛЬЗУЕТСЯ
    def WriteErrorMessageNew(self, ex, ExecId):
        try:
            sess = sessionmaker(bind=self.t_engine)()
            param = {'ERROR_MESSAGE':str(ex), 'ExecId':ExecId, 'Error_Source':'Py'}
            results = sess.execute(SQLQueries.WriteErrorMessageNew, param)
            sess.commit()
        finally:
            sess.close()
       

    #Exec sp_to_send_dv
    @getLogging
    def Sp_to_send_dv(self):
        try:
            t_cursor = self.t_conn.cursor()

            if 'Audit' in self.obj_group:
                sql = SQLQueries.ExecAuditProc.format(self.sp_to_send_dv, self.Execid, self.ds_name_source,
                                                      self.obj_group, self.target_schema, self.target_name)
            else:
                sql = SQLQueries.ExecProc.format(self.sp_to_send_dv, self.Execid, self.ds_name_source)

            t_cursor.execute(sql)
        except Exception as e:
             t_cursor.cancel()
             self.WriteErrorMessage(str(e), self.Execid)
             raise

#ПРОТЕСТИТЬ
    #Exec any sp
    def Exec_any_SP(conn, sp, params):
        try:
            cursor = conn.cursor()
            sql = 'EXEC {0} {1} '.format(sp, params)
            cursor.execute(sql)
        except Exception as e:
             cursor.cancel()
             self.WriteErrorMessage(str(e), self.Execid)
             raise