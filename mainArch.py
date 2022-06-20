from functions import (shipped_lines_query_dp
    , crown_hlg_kits_dp
    , des_opc
    , open_tags
    , inbound_receipts
    , inbound_putaway )

import pandas as pd
import os

# open_tags()
aNumber = 'A339580'
download_path = rf'C:\Users\{aNumber}\Downloads'

# archive_shipped_kits = pd.read_excel(r'C:\Users\A381375\Downloads\3PL OB Archive Data BYH 2022.xlsx', sheet_name='Shipped Kits')
# shipped_lines_query_dp(set_report=archive_shipped_kits)
#
# archived_dp_kits = pd.read_excel(r'C:\Users\A381375\Downloads\3PL OB Archive Data BYH 2022.xlsx', sheet_name='DP Kits')
# crown_hlg_kits_dp(set_report=archived_dp_kits)
#
# archived_description = pd.read_excel(r'C:\Users\A381375\OneDrive - Volvo Group\Onur\3pl-pipelines\opc_des.xlsx')
# des_opc(archived_description)

# archived_receipts = pd.read_excel(os.path.join(download_path, 'Data Archives IB Dashboard.xlsx'), sheet_name='Receipts')
# inbound_receipts(set_report=archived_receipts)

archived_crwn_putaways = pd.read_excel(os.path.join(download_path, 'Data Archives IB Dashboard.xlsx')
                                       ,sheet_name='CRW Putaways')
archived_hlg_lip_putaways = pd.read_excel(os.path.join(download_path, 'Data Archives IB Dashboard.xlsx')
                                          ,sheet_name='HLG LIP Putaways')
inbound_putaway(set_crown_report=archived_crwn_putaways, set_hlg_lip_report=archived_hlg_lip_putaways)


