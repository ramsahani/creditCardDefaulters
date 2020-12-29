import shutil
import _sqlite3
from os import listdir
import os
import csv
from application_logging.logger import App_Logger

class dBOperations:
    """
    This class shall be used for handling all the SQL queries.
    """
    def __init__(self):
        self.path = "Training_Database/"
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/GoodRaw"
        self.logger = App_Logger()


    def dataBaseConncetion(self, DatabaseName):
        """
        This method creates database of the given if database already exits then opens connection to Database.
        :param DatabaseName:
        :return: Connection to the DB
        """

        try:
            conn = _sqlite3.connect(self.path+DatabaseName+'.db')
            file = open('Training_Logs/DataBaseConnectionLog.txt','a+')
            self.logger.log(file, "Opened %s database successfully " % DatabaseName)
            file.close()

        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file, "Error occurred while connecting to database %s " %ConnectionError)
            file.close()
            raise ConnectionError
        return conn
