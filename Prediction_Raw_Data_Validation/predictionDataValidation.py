from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger


class Prediction_Data_validation:
    """
    This class shall be used for handling all the validation done on the Raw Prediction Data!!.
    """

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """
            This method extract as all the relevant information from the pre-defined "Schema" file.
        :return: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Numberofcolumns
        """

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):
        """
            This method contains a manually defined regex based on the "FileName" given in "Schema" file.
            This Regex is used to validate the filename of the prediction data.
        :return: Regex pattern
        """
        # "SampleFileName": "creditCardFraud_021119920_010222.csv"
        regex = "['creditcardsFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):
        """
            This method creates directories to store the Good Data and Bad data after validating the prediction
        :return: None
        """

        try:
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as ex:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataPredictionFolder(self):
        """
            This method deletes the directory made to store the good data after loading in the table.
            Once the good files are loaded in DB, deleting the directory ensures space optimization.
        :return: None
        """

        try:
            path = 'Prediction_Raw_Data_Validated/Good_Raw/'

            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

    def deleteExistingBadDataPredictionFolder(self):
        """
            This method deletes the directory made to store the good data after loading in the table.
            Once the good files are loaded in DB, deleting the directory ensures space optimization.
        :return: None
        """

        try:
            path = 'Prediction_Raw_Data_Validated/Bad_Raw/'

            if os.path.isdir(path):
                shutil.rmtree(path)
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad_Raw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % s)
            file.close()
            raise OSError

