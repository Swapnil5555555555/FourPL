
import time
from uuid import uuid4
from datetime import datetime
import connection

class Logger:
    
    def __init__(self, name_of_instance):
        self.name_of_instance = name_of_instance
        self.db = connection.azure_connection()
        self.cursor = self.db.cursor()
        
        
    def execute_task(self, executable):
        
        function_name = executable.__name__
        start_time = time.time()
        logger_key = uuid4()
        self._log_time(function_name, start_time, True, None, logger_key)
        executable()
        end_time = time.time()
        total_time = end_time - start_time
        self._log_time(function_name, end_time, False, total_time, logger_key)
        
    
    def _log_time(self, name, timestamp, isStartTime, total_time, record_key):
        # TODO: Log details in no-sql-database for optimal speeds
        create_record = """
            INSERT INTO [dbo].[Data_Pipeline_Logs] (
                REPORT_NAME
                ,START_DTSTAMP
                ,RECORD_KEY
            )VALUES
            (   ?
                ,?
                ,?
            )
            """
            
        update_record = """
            UPDATE [dbo].[Data_Pipeline_Logs]
            SET END_DTSTAMP = ? 
                , TOTAL_EXECUTION_TIME_SECONDS = ?
                , UPDATED_AT = ?
            WHERE RECORD_KEY = ?
            """
        if isStartTime:
            params = (name, datetime.fromtimestamp(timestamp), record_key)
            self._commit_to_log(create_record, params)
        else:
            current_datetime = datetime.fromtimestamp(timestamp)
            params = (current_datetime, total_time, current_datetime, record_key)
            self._commit_to_log(update_record, params)
            
    def _commit_to_log(self, sql_query, params):
        
        attempts = 10
        did_not_upload = True
        while attempts and did_not_upload:
            try:
                self.cursor.execute(sql_query, params)
                self.db.commit()
                did_not_upload = False
            except Exception as e:
                print(e)
                attempts -= 1
                self.reconnect()
            
            
    def reconnect(self):
        is_reconnected = False
        while not is_reconnected:
            try:
                self.db = connection.azure_connection()
                self.cursor = self.db.cursor()
                is_reconnected = True
            except:
                time.sleep(3)
            
    def terminate(self):
        try:
            self.db.close()
        except:
            pass
            