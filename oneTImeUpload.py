
import pandas as pd
import queries
import connection
from uuid import uuid4
from datetime import datetime, timedelta
import warnings

oracle_cnxn = connection.oracle_connection()
azure_cnxn = connection.azure_connection()
azure_cursor = azure_cnxn.cursor()


CRN_LIP_PRODUCTIVITY = """
        SELECT  
        'CRW_PROD' AS TYPE, 
        SKU_ID,
        TAG_ID, 
        MAX(DSTAMP) AS DSTAMP, 
        UPDATE_QTY AS QTY
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='MEM'
        AND TO_LOC_ID LIKE 'CR%OT%'
        AND TRUNC(DSTAMP) < TRUNC(SYSDATE-1.19)
        GROUP BY  SKU_ID, TAG_ID, UPDATE_QTY

        UNION ALL

        SELECT 'LIP_PROD' AS TYPE, SKU_ID, TAG_ID, MAX(DSTAMP) AS DSTAMP, UPDATE_QTY AS QTY
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='LIP'
        AND CODE='Shipment'
        AND TRUNC(DSTAMP) < TRUNC(SYSDATE-1.19)
        AND CUSTOMER_ID='8359'
        GROUP BY  SKU_ID, TAG_ID, UPDATE_QTY
"""
INSERT_CRN_LIP_PRODUCTIVITY = """
    INSERT INTO [dbo].[Crw_Lip_Productivity] 
    (
         TYPE
    ,SKU_ID 
    ,TAG_ID
    ,DSTAMP
    ,QTY
    )
    VALUES 
    (
        ?
        ,?
        ,?
        ,?
        ,?
    )
    """
    
def is_none(val):
    if val is None or str(val) == 'NaT' or str(val) == 'NaN' or str(val) == 'nan':
        return None
    else:
        return val

def crown_lipert_productivity():
    
    global azure_cnxn, azure_cursor, oracle_cnxn
    
    
    oracle_sql_query = CRN_LIP_PRODUCTIVITY
    productivity_df = pd.read_sql(oracle_sql_query, oracle_cnxn)

    for index, row in productivity_df.iterrows():
        # if set_report is None:
            # try:
        try:
            azure_cursor.execute(INSERT_CRN_LIP_PRODUCTIVITY,
                                    is_none(row['TYPE']),
                                    is_none(row['SKU_ID']),
                                    is_none(row['TAG_ID']),
                                    is_none(row['DSTAMP']),
                                    is_none(row['QTY']))
            azure_cnxn.commit()
        except Exception as error:
            print(error)
            # TODO: Send an email that a record failed. Log the record
            
            

crown_lipert_productivity()