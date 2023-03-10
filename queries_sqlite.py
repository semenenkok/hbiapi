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



sql_RefAc = """ select id, BK_SourceMediumCode, startDate, endDate, acRate 
                from RefAc    """                    

sql_insert_RefAc = """
            INSERT  INTO RefAc(BK_SourceMediumCode, startDate, endDate, acRate)
            VALUES('{0}', '{1}', '{2}', '{3}');
            """

sql_update_RefAc = """
            UPDATE RefAc
            SET BK_SourceMediumCode = '{1}',
            startDate = '{2}',
            endDate = '{3}',
            acRate = '{4}'
            WHERE id = {0};
            """

sql_delete_RefAc = """
            DELETE FROM RefAc WHERE id = {0}
             """


sql_RefVat = """ select id, startDate, endDate, vatRate 
                from RefVat """                    

sql_upsert_RefVat = """
            INSERT OR REPLACE INTO RefVat(id, startDate, endDate, vatRate)
            VALUES({0}, '{1}', '{2}', {3});
            """
sql_delete_RefVat = """
            DELETE FROM RefVat 
            WHERE id = {0} 
            """


sql_RefVatArm = """ select id, startDate, endDate, vatRate 
                from RefVatArm """                    

sql_upsert_RefVatArm = """
            INSERT OR REPLACE INTO RefVatArm(id, startDate, endDate, vatRate)
            VALUES({0}, '{1}', '{2}', {3});
            """
sql_delete_RefVatArm = """
            DELETE FROM RefVatArm 
            WHERE id = {0}
            """
            
