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
                    FROM AuditResults
                    """
sql_RefAc = """ select BK_SourceMediumCode, startDate, endDate, acRate 
                from RefAc
            """                    

sql_insert_RefAc = """
            INSERT INTO RefAc (BK_SourceMediumCode, startDate, endDate, acRate)
            VALUES ('{0}','{1}','{2}',{3}) """

