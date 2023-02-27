sql_GetAuditResults = """
                    SELECT AuditDate
                        , obj_group
                        , ds_name_source
                        , TableName
                        , Descr
                        , SourceRows
                        , DWHRows
                        , DiffRows
                        , PassedIndicator
                        , 0 as SameResultDaysCnt
                    FROM AuditResults                    """



sql_RefAc = """ select BK_SourceMediumCode, startDate, endDate, acRate 
                from RefAc    """                    

sql_upsert_RefAc = """
            INSERT INTO RefAc(BK_SourceMediumCode, startDate, endDate, acRate) VALUES('{0}','{1}','{2}',{3})
            ON CONFLICT(BK_SourceMediumCode, startDate, endDate) DO UPDATE SET acRate = {3};
            """
sql_delete_RefAc = """
            DELETE FROM RefAc 
            WHERE BK_SourceMediumCode = '{0}'
              and startDate = '{1}'
              and endDate = '{2}'
             """

sql_RefVat = """ select startDate, endDate, vatRate 
                from RefVat """                    

sql_upsert_RefVat = """
            INSERT INTO RefVat(startDate, endDate, vatRate) VALUES('{0}','{1}','{2}')
            ON CONFLICT(startDate, endDate) DO UPDATE SET vatRate = {2};
            """

sql_delete_RefVat = """
            DELETE FROM RefVat 
            WHERE startDate = '{0}'
              and endDate = '{1}'    """


sql_RefVatArm = """ select startDate, endDate, vatRate 
                from RefVatArm """                    

sql_upsert_RefVatArm = """
            INSERT INTO RefVatArm(startDate, endDate, vatRate) VALUES('{0}','{1}','{2}')
            ON CONFLICT(startDate, endDate) DO UPDATE SET vatRate = {2};
            """

sql_delete_RefVatArm = """
            DELETE FROM RefVatArm 
            WHERE startDate = '{0}'
              and endDate = '{1}'    """
            
