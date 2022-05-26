import pandas as pd
import queries
import connection
from uuid import uuid4
from datetime import datetime, timedelta
import warnings

oracle_cnxn = connection.oracle_connection()
azure_cnxn = connection.azure_connection()
azure_cursor = azure_cnxn.cursor()


def is_none(val):
    if val is None or str(val) == 'NaT' or str(val) == 'NaN' or str(val) == 'nan':
        return None
    else:
        return val


def standard_cost():
    curr_date = datetime.now().strftime('%Y-%m-%d')
    report = pd.read_sql(queries.standard_cost_query,
                         oracle_cnxn)  # this has one million rows of data, but does not need refresh often (1 - 3 times a month)
    try:
        for index, row in report.iterrows():
            azure_cursor.execute(queries.standard_cost_insert_query,
                                 (row['SKU_ID']
                                  , row['STANDARD_COST']
                                  , curr_date
                                  , uuid4()
                                  ))
            azure_cnxn.commit()
    except Exception as e:
        print(e)
        #TODO: Log any rows of data that are failing to send... Make sure to queue this reports in email thread

    return


def crown_hlg_kits_dp():
    report = pd.read_sql(queries.crown_hlg_kits_dp_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.dp_kits_insert_query
                                 , (row['REPORT_DATE']
                                    , row['KIT_BUILDER']
                                    , row['COMPONENT_SKU']
                                    , row['ORDER_ID']
                                    , row['LINE_ID']
                                    , row['ORDER_TYPE']
                                    , row['V_BRAND']
                                    , row['SHIP_BY_DATE_']
                                    , is_none(row['SHIPPED_DATE'])
                                    , row['QTY_ORDERED']
                                    , is_none(row['SHIPPED_QTY'])
                                    , row['ORDER_STATUS']
                                    , row['LINE_STATUS']
                                    , uuid4())
                                 )
            azure_cnxn.commit()
        except Exception as e:
            print(e)
            # TODO: Log Missed Row Here, and enable email notification
            pass
    return


def shipped_lines_query_dp():
    report = pd.read_sql(queries.shipped_lines_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.shipped_lines_insert_query,
                                 (row['REPORT_DATE']
                                  , row['3PL']
                                  , row['SKU_ID']
                                  , row['SHIPPED_QTY']
                                  , row['ORDER_TYPE']
                                  , row['ORDER_ID']
                                  , row['LINE_ID']
                                  , row['SHIP_BY_DATE']
                                  , uuid4()))
            azure_cnxn.commit()
        except Exception as e:
            print(e)
            # TODO: Log missed row on distinct report and distribute
            # TODO: Also, indicate that the report will be a part of the email
            pass


def open_kits():
    report = pd.read_sql(queries.open_kits_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:

            azure_cursor.execute(queries.open_kits_insert_query,
                                 (row['REPORT_DATE']
                                  , row['SHIP_BY_DATE']
                                  , row['3PL']
                                  , row['QTY_ORDERED']
                                  , row['SKU_ID']
                                  , row['ORDER_TYPE']
                                  , row['ORDER_ID']
                                  , row['LINE_ID']
                                  , uuid4()))
            azure_cnxn.commit()
        except Exception as e:
            print(e)
            # TODO: Log missed row on distinct report and distribute
            # TODO: Also, indicate that the report will be a part of the email
            pass


def des_opc():
    curr_date = datetime.now().strftime('%Y-%m-%d')
    report = pd.read_sql(queries.des_ops_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.des_ops_insert_query, (
                row['SKU_ID']
                ,row['OPC']
                ,row['DESCRIPTION']
                ,curr_date
                ,uuid4()
            ))

            azure_cnxn.commit()
        except Exception as e:
            print(e)
            #TODO: Log any failed uploads and trigger warning emails


def crown_prepack_rates():
    curr_date = datetime.now().strftime('%Y-%m-%d')
    report = pd.read_sql(queries.crown_prepack_rates_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.crown_prepack_rates_insert_query,
                                 (row['SKU_ID']
                                 ,row['PREPACK_CODE']
                                  ,row['SALES_MULTIPLE']
                                  ,row['PREPACK_RATE']
                                  ,curr_date
                                  ,uuid4()))
            azure_cnxn.commit()
        except Exception as e:
            print(e)
            #TODO: Log any failed uploads and enable email report


def inbound_putaway():
    report_crown = pd.read_sql(queries.inbound_putaway_query, oracle_cnxn)
    for index, row in report_crown.iterrows():
        try:
            azure_cursor.execute(queries.inbound_putaway_insert_query, (
                row['REPORT_DATE']
                ,row['TAG_ID']
                ,row['SKU_ID']
                ,row['PUTAWAY_DATE']
                ,row['PUTAWAY_QTY']
                ,row['BYH_RECEIPT_FROM_CROWN_DATE_']
                ,row['BYH_RECEIPT_QTY_FROM_CROWN']
                ,row['CROWN_TRAILER_LOAD_DATE']
                ,row['CROWN_TRAILER_LOAD_QTY']
                ,uuid4()
            ))
        except Exception as e:
            print(e)
            #TODO: Log Failures on report and trigger email report

    report_hlg_lip = pd.read_sql(queries.hlg_lip_putaways, oracle_cnxn)
    for index, row in report_hlg_lip.iterrows():
        try:
            azure_cursor.execute(queries.hlg_lip_putaways, (
                row['REPORT_DATE']
                ,row['3PL']
                ,row['ASN_ID']
                ,row['TAG_ID']
                ,row['SKU_ID']
                ,row['SUPPLIER_ID']
                ,row['QTY_RECEIVED']
                ,row['QTY_PUTAWAY']
                ,row['RECEIPT_DATE']
                ,row['PUTAWAY_DATE']
                ,row['ASN_CREATION_DATE']
                ,row['ASN_COMPLETION_DATE']
                ,uuid4()
            ))
        except Exception as e:
            print(e)
            #TODO: Log Failures on report and trigger email report

