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

sql_insert_RefAc = """
            INSERT INTO RefAc (BK_SourceMediumCode, startDate, endDate, acRate)
            VALUES ('{0}','{1}','{2}',{3}) """

sql_insert_RefAc = """
          UPDATE RefAc
          SET acRate = {3}
          WHERE BK_SourceMediumCode = '{0}'
            AND startDate = '{1}' AND endDate = '{2}'  """



sql_RefVat = """ select startDate, endDate, vatRate 
                from RefVat """                    

sql_insert_RefVat = """
            INSERT RefVat (startDate, endDate, vatRate)
            VALUES  ('{0}','{1}',{}) """

sql_update_RefVat = """
            UPDATE RefVat
            SET vatRate = {2}
            WHERE startDate = '{0}' AND endDate = '{1}' """


sql_RefVatArm = """ select startDate, endDate, vatRate 
                from RefVatArm """                    

sql_insert_RefVatArm = """
            INSERT RefVatArm (startDate, endDate, vatRate)
            VALUES  ('{0}','{1}',{}) """

sql_update_RefVatArm = """
            UPDATE RefVatArm
            SET vatRate = {2}
            WHERE startDate = '{0}' AND endDate = '{1}' """



            
