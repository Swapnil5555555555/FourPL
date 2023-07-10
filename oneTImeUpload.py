
import pandas as pd
import queries
import connection
import json
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
        AND TRUNC(DSTAMP) >= TRUNC(SYSDATE - 3, 'DD')
        GROUP BY  SKU_ID, TAG_ID, UPDATE_QTY

        UNION ALL

        SELECT 'LIP_PROD' AS TYPE, SKU_ID, TAG_ID, MAX(DSTAMP) AS DSTAMP, UPDATE_QTY AS QTY
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='LIP'
        AND CODE='Shipment'
        AND TRUNC(DSTAMP) < TRUNC(SYSDATE-1.19)
        AND TRUNC(DSTAMP) >= TRUNC(SYSDATE - 3, 'DD')
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
    
    
def log_missed_record(record):
    with open('readme.txt', 'w') as outfile:
        record_json = record.to_json()
        json.dump(record_json, outfile)
        
        
def crown_lipert_productivity():
    
    global azure_cnxn, azure_cursor, oracle_cnxn
    
    
    oracle_sql_query = CRN_LIP_PRODUCTIVITY
    productivity_df = pd.read_sql(oracle_sql_query, oracle_cnxn)
    
    def upload(row):
        azure_cursor.execute(INSERT_CRN_LIP_PRODUCTIVITY,
                                    is_none(row['TYPE']),
                                    is_none(row['SKU_ID']),
                                    is_none(row['TAG_ID']),
                                    is_none(row['DSTAMP']),
                                    is_none(row['QTY']))
        azure_cnxn.commit()
        
    print(productivity_df.shape)
    for index, row in productivity_df.iterrows():
        try:
            upload(row)
        except Exception as error:
            print(error)
            try:
                azure_cnxn.close()
            except:
                print('unable to close connection')
             
            azure_cnxn = connection.azure_connection()
            azure_cursor = azure_cnxn.cursor()
            upload(row)
            
    
            
            

def lippert_aging_gr():
    
    global azure_cnxn, azure_cursor, oracle_cnxn
    
    
    oracle_sql_query = queries.LIP_AGING_GR
    productivity_df = pd.read_sql(oracle_sql_query, oracle_cnxn)


    def upload(row):
        azure_cursor.execute(queries.INSERT_LIP_AGING,
                                    is_none(row['REPORT_DATE']),
                                    is_none(row['SITE_ID']),
                                    is_none(row['IS_RETURNS']),
                                    is_none(row['DESCRIPTION']),
                                    is_none(row['ORIGIN']),
                                    is_none(row['SUPPLIER']),
                                    is_none(row['TAG']),
                                    is_none(row['SKU']),
                                    is_none(row['LOCATION']),
                                    is_none(row['PALLET']),
                                    is_none(row['QOH']),
                                    is_none(row['LAST_MOVE']),
                                    is_none(row['RECEIVED_DATE']),
                                    is_none(row['CONDITION_CODE']))
        azure_cnxn.commit()

    print(productivity_df.shape)
    for index, row in productivity_df.iterrows():
        
        try:
            upload(row)
        except Exception as error:
            print(error)
            try:
                azure_cnxn.close()
            except:
                print('unable to close connection')
            print(row)
            azure_cnxn = connection.azure_connection()
            azure_cursor = azure_cnxn.cursor()
            upload(row)
    
    
# crown_lipert_productivity()
# lippert_aging_gr()