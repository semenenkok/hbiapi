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
                   , SameResultDaysCnt
                    FROM [aud].[fn_AuditResults_on_Date]('{0}')
                    """
sql_RefAc = """ select BK_SourceMediumCode, startDate, endDate, acRate 
                from bv.RefAc
            """                    
