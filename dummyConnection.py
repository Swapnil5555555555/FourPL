import cx_Oracle
from dotenv import load_dotenv
import os

load_dotenv()
cx_Oracle.init_oracle_client(config_dir=os.environ.get('ORACLE_NETWORK_CONFIG'))


class dummyConnection:
    
    def execute(self, *args):
        return self
    def commit(self, *args):
        return self
    def cursor(self, *args):
        return self
    def close(self):
        return self

def azure_connection():
   
    return dummyConnection()


def oracle_connection():
    username = os.environ.get('ORACLE_USERNAME')
    password = os.environ.get('ORACLE_PASSWORD')
    dsn = os.environ.get('ORACLE_DSN')
    return cx_Oracle.connect(user=username, password=password, dsn=dsn)



