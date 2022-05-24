import pandas as pd

import connection
import pandas
import warnings

oracle_cnxn = connection.oracle_connection()
azure_cnxn = connection.azure_connection()


def standard_cost():
    sql_query = f"""
    SELECT 
        SKU_ID, 
        STANDARD_COST	
    FROM DCSDBA.V_SKU_PROPERTIES	
    WHERE SITE_ID='MEM'	
        AND STANDARD_COST IS NOT NULL	
    """
    report = pd.read_sql(sql_query, oracle_cnxn)
    for index, row in report.iterrows():
        #TODO: Upload to Azure SQL Database
        pass

    return



