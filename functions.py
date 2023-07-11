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
    global azure_cnxn, azure_cursor

    curr_date = datetime.now().strftime('%Y-%m-%d')
    report = pd.read_sql(queries.standard_cost_query,
                         oracle_cnxn)  # this has one million rows of data, but does not need refresh often (1 - 3 times a month)

    print(report.shape)
    def upload(row):
        azure_cursor.execute(queries.standard_cost_insert_query,
                                 (row['SKU_ID']
                                  , row['STANDARD_COST']
                                  , curr_date
                                  , uuid4()
                                  ))
        azure_cnxn.commit()
    try:
        for index, row in report.iterrows():
           upload(row)
    except Exception as e:
        print(e)
        try:
            azure_cnxn.close()
        except Exception as error:
            print(error)


        azure_cnxn = connection.azure_connection()
        azure_cursor = azure_cnxn.cursor()

        try:
            upload(row)
        except Exception as error:
            print('unable to upload row', row)
        #TODO: Log any rows of data that are failing to send... Make sure to queue this reports in email thread

    return


def crown_hlg_kits_dp(set_report=None):

    if set_report is not None:
        report = set_report
    else:
        report = pd.read_sql(queries.crown_hlg_kits_dp_query, oracle_cnxn)

    report_size = report.shape[0]
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
            report_size -= 1
            print('remaining uploads:', report_size)

        except Exception as e:
            print(e)
            # TODO: Log Missed Row Here, and enable email notification
            pass
    return


def shipped_lines_query_dp(set_report=None):

    if set_report is not None:
        report = set_report
    else:
        report = pd.read_sql(queries.shipped_lines_query, oracle_cnxn)
    size_report = report.shape[0]
    for index, row in report.iterrows():
        try:
            print('uploading shipped lines', size_report)
            azure_cursor.execute(queries.shipped_lines_insert_query,
                                 (row['REPORT_DATE']
                                  , row['3PL']
                                  , row['SKU_ID']
                                  , row['SHIPPED_QTY']
                                  , row['ORDER_TYPE']
                                  , row['ORDER_ID']
                                  , row['LINE_ID']
                                  , row['SHIP_BY_DATE']
                                  , row['SHIPPED_DATE']
                                  , uuid4()))
            azure_cnxn.commit()
            size_report -= 1
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


def des_opc(set_report=None):
    curr_date = datetime.now().strftime('%Y-%m-%d')
    if set_report is not None:
        report = set_report
    else:
        report = pd.read_sql(queries.des_ops_query, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.des_ops_insert_query, (
                is_none(row['SKU_ID'])
                ,is_none(row['OPC'])
                ,is_none(row['DESCRIPTION'])
                ,curr_date
                ,uuid4()
                ,is_none(row['BRAND_OPC'])
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


def inbound_putaway_hlg_lip(set_hlg_lip_report=None):

    global azure_cnxn, azure_cursor

    if set_hlg_lip_report is not None:
        report_hlg_lip = set_hlg_lip_report
        skip_dates = (pd.read_sql(queries.hlg_lip_putaways_dates, azure_cnxn))['REPORT_DATE'].to_list()
        report_hlg_lip['PUTAWAY_DATE'] = pd.to_datetime(report_hlg_lip['PUTAWAY_DATE'])
        report_hlg_lip['RECEIPT_DATE'] = pd.to_datetime(report_hlg_lip['RECEIPT_DATE'])
        report_hlg_lip['ASN_CREATION_DATE'] = pd.to_datetime(report_hlg_lip['ASN_CREATION_DATE'])
        report_hlg_lip['ASN_COMPLETION_DATE'] = pd.to_datetime(report_hlg_lip['ASN_COMPLETION_DATE'])

        report_hlg_lip['QTY_RECEIVED'] = (report_hlg_lip['QTY_RECEIVED'].fillna(0)).astype(
            'int64')
        report_hlg_lip['QTY_PUTAWAY'] = (report_hlg_lip['QTY_PUTAWAY'].fillna(0)).astype('int64')
    else:
        report_hlg_lip = pd.read_sql(queries.hlg_lip_putaways, oracle_cnxn)
        skip_dates = None

    skipped = 0
    for index, row in report_hlg_lip.iterrows():
        if set_hlg_lip_report is not None and skip_dates is not None:
            this_date = row['REPORT_DATE'].date()
            if this_date not in skip_dates:
                try:
                    azure_cursor.execute(queries.hlg_lip_putaways_insert, (
                        row['REPORT_DATE']
                        , is_none(row['3PL'])
                        , is_none(row['ASN_ID'])
                        , is_none(row['TAG_ID'])
                        , is_none(row['SKU_ID'])
                        , is_none(row['SUPPLIER_ID'])
                        , is_none(row['QTY_RECEIVED'])
                        , is_none(row['QTY_PUTAWAY'])
                        , is_none(row['RECEIPT_DATE'])
                        , is_none(row['PUTAWAY_DATE'])
                        , is_none(row['ASN_CREATION_DATE'])
                        , is_none(row['ASN_COMPLETION_DATE'])
                        , uuid4()
                    ))
                    azure_cnxn.commit()
                except:
                    try:
                        azure_cursor.close()
                    except:
                        pass
                    try:
                        azure_cnxn.close()
                    except:
                        pass
                    try:
                        azure_cnxn = connection.azure_connection()
                        azure_cursor = azure_cnxn.cursor()
                        azure_cursor.execute(queries.hlg_lip_putaways_insert, (
                            row['REPORT_DATE']
                            , is_none(row['3PL'])
                            , is_none(row['ASN_ID'])
                            , is_none(row['TAG_ID'])
                            , is_none(row['SKU_ID'])
                            , is_none(row['SUPPLIER_ID'])
                            , is_none(row['QTY_RECEIVED'])
                            , is_none(row['QTY_PUTAWAY'])
                            , is_none(row['RECEIPT_DATE'])
                            , is_none(row['PUTAWAY_DATE'])
                            , is_none(row['ASN_CREATION_DATE'])
                            , is_none(row['ASN_COMPLETION_DATE'])
                            , uuid4()
                        ))
                        azure_cnxn.commit()
                    except Exception as e:
                        print('unable to upload row', row, 'due to this error:', e)
                        continue
            else:
                skipped += 1
                print('Has date, number of skipped:', skipped)
                continue
        else:
            try:
                azure_cursor.execute(queries.hlg_lip_putaways_insert, (
                    row['REPORT_DATE']
                    ,is_none(row['3PL'])
                    ,is_none(row['ASN_ID'])
                    ,is_none(row['TAG_ID'])
                    ,is_none(row['SKU_ID'])
                    ,is_none(row['SUPPLIER_ID'])
                    ,is_none(row['QTY_RECEIVED'])
                    ,is_none(row['QTY_PUTAWAY'])
                    ,is_none(row['RECEIPT_DATE'])
                    ,is_none(row['PUTAWAY_DATE'])
                    ,is_none(row['ASN_CREATION_DATE'])
                    ,is_none(row['ASN_COMPLETION_DATE'])
                    ,uuid4()
                ))
                azure_cnxn.commit()
            except Exception as e:
                print(e)
                continue


def inbound_putaway_crown(set_crown_report=None):
    global azure_cnxn, azure_cursor

    if set_crown_report is not None:
        report_crown = set_crown_report
        skip_dates = (pd.read_sql(queries.crown_putaways_dates, azure_cnxn))['REPORT_DATE'].to_list()
        report_crown['PUTAWAY_DATE'] = pd.to_datetime(report_crown['PUTAWAY_DATE'])
        report_crown['BYH_RECEIPT_FROM_CROWN_DATE_'] = pd.to_datetime(report_crown['BYH_RECEIPT_FROM_CROWN_DATE_'])
        report_crown['CROWN_TRAILER_LOAD_DATE'] = pd.to_datetime(report_crown['CROWN_TRAILER_LOAD_DATE'])
        report_crown['BYH_RECEIPT_QTY_FROM_CROWN'] = (report_crown['BYH_RECEIPT_QTY_FROM_CROWN'].fillna(0)).astype(
            'int64')
    else:
        report_crown = pd.read_sql(queries.inbound_putaway_query, oracle_cnxn)
        skip_dates = None

    skipped = 0

    for index, row in report_crown.iterrows():
        if set_crown_report is not None and skip_dates is not None:  # We can assume that we will always have archive
            if row['REPORT_DATE'].date() not in skip_dates:
                this_date = row['REPORT_DATE'].date()
                try:
                    azure_cursor.execute(queries.inbound_putaway_insert_query, (
                        this_date
                        , is_none(row['TAG_ID'])
                        , is_none(row['SKU_ID'])
                        , is_none(row['PUTAWAY_DATE'])
                        , is_none(row['PUTAWAY_QTY'])
                        , is_none(row['BYH_RECEIPT_FROM_CROWN_DATE_'])
                        , is_none(row['BYH_RECEIPT_QTY_FROM_CROWN'])
                        , is_none(row['CROWN_TRAILER_LOAD_DATE'])
                        , is_none(row['CROWN_TRAILER_LOAD_QTY'])
                        , uuid4()
                    ))
                    azure_cnxn.commit()
                except Exception as e:
                    try:
                        azure_cursor.close()  # Close cursor
                    except:
                        pass
                    try:
                        azure_cnxn.close()  # Close connection
                    except:
                        pass

                    try:
                        azure_cnxn = connection.azure_connection()  # Establish a new connection
                        azure_cursor = azure_cnxn.cursor()  # Open a new cursor
                        azure_cursor.execute(queries.inbound_putaway_insert_query, (  # attempt to upload record again
                            this_date
                            , is_none(row['TAG_ID'])
                            , is_none(row['SKU_ID'])
                            , is_none(row['PUTAWAY_DATE'])
                            , is_none(row['PUTAWAY_QTY'])
                            , is_none(row['BYH_RECEIPT_FROM_CROWN_DATE_'])
                            , is_none(row['BYH_RECEIPT_QTY_FROM_CROWN'])
                            , is_none(row['CROWN_TRAILER_LOAD_DATE'])
                            , is_none(row['CROWN_TRAILER_LOAD_QTY'])
                            , uuid4()
                        ))
                        azure_cnxn.commit()
                    except Exception as e:
                        print('Unable to upload row', row, 'due to this error:', e)
                        pass
            else:
                skipped += 1
                print('Has date, number of rows skipped:', skipped)
                continue
        else:
            try:
                azure_cursor.execute(queries.inbound_putaway_insert_query, (
                    row['REPORT_DATE']
                    , is_none(row['TAG_ID'])
                    , is_none(row['SKU_ID'])
                    , is_none(row['PUTAWAY_DATE'])
                    , is_none(row['PUTAWAY_QTY'])
                    , is_none(row['BYH_RECEIPT_FROM_CROWN_DATE_'])
                    , is_none(row['BYH_RECEIPT_QTY_FROM_CROWN'])
                    , is_none(row['CROWN_TRAILER_LOAD_DATE'])
                    , is_none(row['CROWN_TRAILER_LOAD_QTY'])
                    , uuid4()
                ))
                azure_cnxn.commit()
            except Exception as e:
                print(row)
                print(e)


def inbound_receipts(set_report=None):
    global azure_cnxn, azure_cursor
    if set_report is not None:
        report = set_report
        skip_dates = (pd.read_sql(queries.receipt_dates, azure_cnxn))['REPORT_DATE'].to_list()
    else:
        report = pd.read_sql(queries.inbound_receipts, oracle_cnxn)
        skip_dates = None


    skipped = 0

    for index, row in report.iterrows():
        if set_report is not None and skip_dates is not None and len(skip_dates) > 0:
            if row['REPORT_DATE'].date() not in skip_dates:
                try:
                    azure_cursor.execute(queries.inbound_receipts_insert,
                                         (
                                             row['REPORT_DATE']
                                             , is_none(row['3PL'])
                                             , is_none(row['DATE_'])
                                             , is_none(row['TAG_ID'])
                                             , is_none(row['SKU_ID'])
                                             , is_none(row['SUPPLIER_ID'])
                                             , is_none(row['QTY'])
                                             , uuid4()
                                         ))
                    azure_cnxn.commit()
                except Exception as e:
                    try:
                        azure_cursor.close()
                        azure_cnxn.close()
                    except:
                        pass
                    try:
                        azure_cnxn = connection.azure_connection()
                        azure_cursor = azure_cnxn.cursor()
                        azure_cursor.execute(queries.inbound_receipts_insert,
                                             (
                                                 row['REPORT_DATE']
                                                 , is_none(row['3PL'])
                                                 , is_none(row['DATE_'])
                                                 , is_none(row['TAG_ID'])
                                                 , is_none(row['SKU_ID'])
                                                 , is_none(row['SUPPLIER_ID'])
                                                 , is_none(row['QTY'])
                                                 , uuid4()
                                             ))
                        azure_cnxn.commit()
                    except Exception as e:
                        print('COULD NOT UPLOAD THIS RECORD', row, e)
                        pass
            else:
                skipped += 1
                print('Has date, number of skipped:', skipped)
                pass
        else:
            try:
                azure_cursor.execute(queries.inbound_receipts_insert,
                                     (
                                         row['REPORT_DATE']
                                         ,is_none(row['3PL'])
                                         ,is_none(row['DATE_'])
                                         ,is_none(row['TAG_ID'])
                                         ,is_none(row['SKU_ID'])
                                         ,is_none(row['SUPPLIER_ID'])
                                         ,is_none(row['QTY'])
                                         ,uuid4()
                                     ))
                azure_cnxn.commit()
            except Exception as e:
                print(e)
                continue
                #TODO: Log missed rows in distinct report and distribute...
                #TODO: Ensure that the information can be distributed...


def open_asns():
    report = pd.read_sql(queries.open_asns, oracle_cnxn)
    for index, row in report.iterrows():
        try:

            azure_cursor.execute(queries.open_asns_insert, (
                row['REPORT_DATE']
                ,is_none(row['3PL'])
                ,is_none(row['ASN_ID'])
                ,is_none(row['SKU_ID'])
                ,is_none(row['SUPPLIER_ID'])
                ,is_none(row['QTY_DUE'])
                ,is_none(row['ASN_CREATION_DATE'])
                ,is_none(row['ASN_STATUS'])
                ,uuid4()
            ))
            azure_cnxn.commit()
        except Exception as e:
            print(e)
            #TODO: Make sure that all the information was distributed correctly if inpossble include it in the email report


def open_tags():
    report = pd.read_sql(queries.open_tags_hlg_lip, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.open_tags_hlg_lip_insert, (
                row['REPORT_DATE']
                ,is_none(row['3PL'])
                ,is_none(row['PALLET_ID'])
                ,is_none(row['SKU_ID'])
                ,is_none(row['LOCATION_ID'])
                ,is_none(row['RECEIPT_DSTAMP'])
                ,is_none(row['MOVE_DSTAMP'])
                ,is_none(row['QTY'])
                ,is_none(row['TAG_ID'])
                ,uuid4()
            ))
            azure_cnxn.commit()
        except Exception as e:
            print(e)

    report = pd.read_sql(queries.crown_open_tags, oracle_cnxn)
    for index, row in report.iterrows():
        try:
            azure_cursor.execute(queries.open_tags_hlg_lip_insert, (
                row['REPORT_DATE']
                , is_none(row['3PL'])
                , is_none(row['PALLET_ID'])
                , is_none(row['SKU_ID'])
                , is_none(row['LOCATION_ID'])
                , is_none(row['RECEIPT_DSTAMP'])
                , is_none(row['MOVE_DSTAMP'])
                , is_none(row['QTY'])
                , is_none(row['TAG_ID'])
                , uuid4()
            ))
            azure_cnxn.commit()
        except Exception as e:
            print(e)

            #TODO: Log information and trigger email if failure
    #TODO: Need to add crown flow to this automation


def three_pl_cost(set_report=None):

    global azure_cnxn, azure_cursor
    failed = 0
    if set_report is not None:
        report = set_report
        dates = None
    else:
        report = pd.read_sql(queries.THREE_PL_COST, oracle_cnxn)
    for index, row in report.iterrows():
        if set_report is None:
            try:
                azure_cursor.execute(queries.INSERT_DBO_COST_THREEPL,
                                     row['REPORT_DATE'],
                                     is_none(row['3PL']),
                                     is_none(row['CODE']),
                                     is_none(row['TRANSACTION_DATE']),
                                     is_none(row['TAG_ID']),
                                     is_none(row['SKU_ID']),
                                     is_none(row['SUPPLIER_ID']),
                                     is_none(row['QTY']),
                                     uuid4())
            except Exception as e:
                failed += 1
                print(e)
                pass
        elif set_report is not None:
            try:
                azure_cursor.execute(queries.INSERT_DBO_COST_THREEPL,
                                     row['REPORT_DATE'],
                                     is_none(row['3PL']),
                                     is_none(row['CODE']),
                                     is_none(row['TRANSACTION_DATE']),
                                     is_none(row['TAG_ID']),
                                     is_none(row['SKU_ID']),
                                     is_none(row['SUPPLIER_ID']),
                                     is_none(row['QTY']),
                                     uuid4())
                azure_cnxn.commit()
            except Exception as e:
                failed += 1
                print('this row failed', row)
                try:
                    azure_cnxn.close()
                except:
                    pass
                try:
                    azure_cursor.close()
                except:
                    pass

                try:
                    azure_cnxn = connection.azure_connection()
                    azure_cursor = azure_cnxn.cursor()
                    azure_cursor.execute(queries.INSERT_DBO_COST_THREEPL,
                                         row['REPORT_DATE'],
                                         is_none(row['3PL']),
                                         is_none(row['CODE']),
                                         is_none(row['TRANSACTION_DATE']),
                                         is_none(row['TAG_ID']),
                                         is_none(row['SKU_ID']),
                                         is_none(row['SUPPLIER_ID']),
                                         is_none(row['QTY']),
                                         uuid4())
                    azure_cnxn.commit()
                except Exception as e:
                    print(f'Was not able to upload: {row} due to the following error: {e}')
                    pass
        else:
            failed += 1
            print('not doing anything with this record', row)
        
        print('FAILED ROWS', failed)





def crown_work_in_process(set_report=None):
    global azure_cnxn, azure_cursor

    if set_report is not None:

        report = set_report
        dates = (pd.read_sql(queries.DATES_CROWN_WORK_IN_PROCESS, oracle_cnxn))['REPORT_DATE'].to_list()
        dates = set(dates)

    else:

        report = pd.read_sql(queries.WORK_IN_PROGRESS, oracle_cnxn)


    for index, row in report.iterrows():
        if set_report is None:
            try:
                azure_cursor.execute(queries.INSERT_DBO_CROWN_WORK_IN_PROCESS,
                                     row['REPORT_DATE'],
                                     is_none(row['RECEIVED DATE']),
                                     is_none(row['LAST_MOVE']),
                                     is_none(row['CROWN_ENTRY_DATE']),
                                     is_none(row['Returns?']),
                                     is_none(row['PALLET']),
                                     is_none(row['TAG']),
                                     is_none(row['BINLOC']),
                                     is_none(row['SKU']),
                                     is_none(row['QOH']),
                                     is_none(row['DESCRIPTION']),
                                     is_none(row['ORIGIN']),
                                     is_none(row['SUPPLIER']),
                                     is_none(row['SUPP_PALLET']),
                                     is_none(row['CONDITION_CODE']),
                                     uuid4())
                azure_cnxn.commit()
            except Exception as e:
                pass
        else:
            curr_date = row['REPORT_DATE']
            if curr_date in dates:
                continue

            try:
                azure_cursor.execute(queries.INSERT_DBO_CROWN_WORK_IN_PROCESS,
                                     row['REPORT_DATE'],
                                     is_none(row['RECEIVED DATE']),
                                     is_none(row['LAST_MOVE']),
                                     is_none(row['CROWN_ENTRY_DATE']),
                                     is_none(row['Returns?']),
                                     is_none(row['PALLET']),
                                     is_none(row['TAG']),
                                     is_none(row['BINLOC']),
                                     is_none(row['SKU']),
                                     is_none(row['QOH']),
                                     is_none(row['DESCRIPTION']),
                                     is_none(row['ORIGIN']),
                                     is_none(row['SUPPLIER']),
                                     is_none(row['SUPP_PALLET']),
                                     is_none(row['CONDITION_CODE']),
                                     uuid4())
                azure_cnxn.commit()
            except:

                azure_cursor.close()



def crown_lipert_productivity():
    
    global azure_cnxn, azure_cursor, oracle_cnxn
    
    
    oracle_sql_query = queries.CRN_LIP_PRODUCTIVITY
    productivity_df = pd.read_sql(oracle_sql_query, oracle_cnxn)
    
    def upload(row):
        azure_cursor.execute(queries.INSERT_CRN_LIP_PRODUCTIVITY,
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
    
