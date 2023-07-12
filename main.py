from connection import oracle_connection
import functions as func

# Outbound Flows
func.crown_hlg_kits_dp()
func.shipped_lines_query_dp()
func.open_kits()

# Byhalia Inbound
func.inbound_putaway_hlg_lip()
func.inbound_putaway_crown()
func.inbound_receipts()
func.open_asns()
func.open_tags()

# General Flows
func.three_pl_cost()
func.crown_work_in_process()
func.crown_lipert_productivity()
func.lippert_aging_gr()

# Terminate cnxn to server
func.oracle_cnxn.close()
func.azure_cnxn.close()