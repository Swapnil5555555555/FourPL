from functions import shipped_lines_query_dp, crown_hlg_kits_dp
import pandas as pd

archive_shipped_kits = pd.read_excel(r'C:\Users\A381375\Downloads\3PL OB Archive Data BYH 2022.xlsx', sheet_name='Shipped Kits')
shipped_lines_query_dp(set_report=archive_shipped_kits)

archived_dp_kits = pd.read_excel(r'C:\Users\A381375\Downloads\3PL OB Archive Data BYH 2022.xlsx', sheet_name='DP Kits')
crown_hlg_kits_dp(set_report=archived_dp_kits)
