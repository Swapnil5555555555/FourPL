import cx_Oracle
import pyodbc
import os
from dotenv import load_dotenv
load_dotenv()
cx_Oracle.init_oracle_client(config_dir=os.environ.get('ORACLE_NETWORK_CONFIG'))




def azure_connection():
    username = os.environ.get('AZURE_USERNAME')
    password = os.environ.get('AZURE_PASSWORD')
    database = os.environ.get('AZURE_DATABASE')
    server = os.environ.get('AZURE_SERVER')
    driver = os.environ.get('AZURE_DRIVER')
    return pyodbc.connect(f'Driver={driver};Server={server};Database={database};Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')


def oracle_connection():
    username = os.environ.get('ORACLE_USERNAME')
    password = os.environ.get('ORACLE_PASSWORD')
    dsn = os.environ.get('ORACLE_DSN')
    return cx_Oracle.connect(user=username, password=password, dsn=dsn)



