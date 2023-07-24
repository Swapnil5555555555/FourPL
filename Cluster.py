from Logger import Logger

"""Cluster of jobs that need to be executed
"""
class Cluster:
    
    def __init__(self, cluster_name, list_of_executes):
        self.cluster_name = cluster_name
        self.list_of_executes = list_of_executes
    
    """task that needs to be ran
    """
    def execute(self):
        logger = Logger(self.cluster_name)
        for function in self.list_of_executes:
            logger.execute_task(function)
            