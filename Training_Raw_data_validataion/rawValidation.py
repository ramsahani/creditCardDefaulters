import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class Raw_Data_validation:
    """
        This is class shall be used for handling all the validation on Raw training data.!!


    """
    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path='schema_training.json'
        self.logger = App_Logger()


    def valuesFromSchema(self):
        """
        This method extracts all the relevant information from the pre-defined 'Schema' file.
        :return: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberOfColumns
        """

        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
            pattern= dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names=dic['ColName']
            NumberOfColumns = dic['NumberofColumns']

            file = open('Training_Logs/valuesfromSchemaValidationLog.txt','a+')
            message="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile +"\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + '\t' + "NumberOfColumns:: %s" % NumberOfColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file=open("Training_Logs/valuesfromSchemValidationLog.txt",'a+')
            self.logger.log(file, "ValueError:: Value not found inside schema_training.json")
            file.close()
            raise ValueError
        except KeyError as e:
            file = open("Training_Logs/valuesfromSchemaValidataionLog.txt",'a+')
            self.logger.log(file, "KeyError: Key value error incorrect key passed,details::%s"%e)
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt",'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfTimeStampInFile,LengthOfDateStampInFile, NumberOfColumns, column_names

    def manualRegexCreation(self):
        """
        This method creates a regex based expression on the "FileName" given in "Schema"file.
         This Regex is used to validate the filename of the training data.
        :return: Regex pattern
        """
        #SampleFileName": "creditCardFraud_021119920_010222.csv"

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex