from queries import BACKTRACK_CRWN_COST
from functions import three_pl_cost
import connection
import pandas as pd



if __name__ == "__main__":
    oracle_cnxn = connection.oracle_connection()
    print('uploading data')
    report = pd.read_sql(BACKTRACK_CRWN_COST, oracle_cnxn)
    print(report)
    three_pl_cost(report)