import multiprocessing as mp
import functions as func
from Cluster import Cluster


if __name__ == "__main__":
    
    task_one = [func.crown_work_in_process, func.crown_lipert_productivity, func.crown_hlg_kits_dp, func.inbound_putaway_crown]
    task_two = [func.lippert_aging_gr, func.open_kits, func.shipped_lines_query_dp, func.inbound_receipts]
    task_three = [func.open_tags, func.inbound_putaway_hlg_lip, func.three_pl_cost, func.open_asns]
    
    cluster_one = Cluster('Cluster One', task_one)
    cluster_two = Cluster('Cluster Two', task_two)
    cluster_three = Cluster('Cluster Three', task_three)
    
    
    process_one = mp.Process(target=cluster_one.execute)
    process_two = mp.Process(target=cluster_two.execute)
    process_three = mp.Process(target=cluster_three.execute)
    
    process_one.start()
    process_two.start()
    process_three.start()

    
    # Terminate cnxn to server
    func.oracle_cnxn.close()
    func.azure_cnxn.close()


