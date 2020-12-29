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
            NumberofColumns = dic['NumberofColumns']

            file = open('Training_Logs/valuesfromSchemaValidationLog.txt','a+')
            message="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile +"\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + '\t' + "NumberOfColumns:: %s" % NumberofColumns + "\n"
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

        return LengthOfTimeStampInFile,LengthOfDateStampInFile, NumberofColumns, column_names

    def manualRegexCreation(self):
        """
        This method creates a regex based expression on the "FileName" given in "Schema"file.
         This Regex is used to validate the filename of the training data.
        :return: Regex pattern
        """
        #SampleFileName": "creditCardFraud_021119920_010222.csv"

        regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def createDirectoryForGoodBadRawData(self):
        """
        This method creates directories to store teh Good Data and Bad Data after validating the training data.

        :return: Nonde
        """

        try:
            path=os.path.join("Training_Raw_files_validated/","Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/","Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while creating Directory :: %s" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        """
            This method deletes the directory made to store the Good Data after
            loading the data in the table. Once the good data files are loaded in
            database, deleting the directory ensures space optimization.
        :return: None
        """

        try:
            path= 'Training_Raw_files_validated/'
            if os.path.isdir(path+ 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file= open("Training_Logs/GeneralLog.txt",'a+')
                self.logger.log(file,"Good_Raw directory deleted successfully!!")
                file.close()

        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,'Error while Deleting Directory :: %s' % s)
            file.close()

    def deleteExistingBadDataTrainingFolder(self):
        """
        This method deletes the directory made to store the bad Data.
        :return: None
        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def moveBadFilesToArchieveBad(self):
        """
            This method deletes the directory made to store the Bad Data after moving the data in an archive folder .
            We archive the bad files to send them back to the client for invalid data issue.
        :return: None
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest= 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, 'Bad Files moved to archive folder')
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path+ 'Bad_Raw/'):
                    shutil.rmtree(path+ 'Bad_Raw/')
            self.logger.log(file, 'Bad Raw Data Folder Deleted Successfully!!')
            file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file, "Error while moving bad data in archived folder %:" % e)
            file.close()
            raise e


    def validationFileName(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
        This function validates the name of the training csv files as per given schema.!!
        Regex pattern is used to do the validation. If name do not match then it is send to Bad Raw data folder
        else Good Raw data folder.
        :param regex:
        :param LengthOfDateStampInFile:
        :param LengthOfTimeStampInFile:
        :return: None
        """
        #delete teh directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        #create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f= open("Training_Logs/nameValidationLog.txt",'a+')
            for filename in onlyfiles:
                if(re.match(regex, filename)):
                    splitAtDot = re.split('.csv',filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2])== LengthOfTimeStampInFile:
                            shutil.copy('Training_Batch_Files/' + filename, "Training_Raw_files_validated/Good_Raw" )
                            self.logger.log(f, "Valida File Name !! File moved to Good Raw filder:: %" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/"+filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name !! File moved to Bad Raw Data: %s" % filename)

                    else:
                        shutil.copy("Training_Batch_Files/"+filename,"Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name !! File moved to Bad Raw Data: %s" %filename)

                f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, 'Error occurred while validating Filename %s' % e)
            f.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
        """
        This function validates the  number of the columns in the csv files as per given in schema.
        if not same then file is moved to Bad Raw data else kept in Good Raw data.


        :param NumberofColumns:
        :return:
        """

        try:
            f = open("Training_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, 'Column Length Validation Started !!')
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns :
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw"+file, 'Training_Raw_file_validated/Bad_Raw')
                    self.logger.log(f,"Invalid Column Length !! File moved to Bad Raw Folder:: %s" % file)
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, "Error occurred while moving the file :: %s " % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
            This function validates if any column in the csv file has all values missing.
            if all the values are missing, the file is not suitable for preprocessing.
            Such files are moved to bad raw data.
            The csv file is missing the first column name , this function changes missing name to 'creditCardFraud'.
        :return: None
        """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt",'a+')
            self.logger.log(f, "Missing Values validation Started!!")

            for file in listdir("Training_Raw_files_validated/Good_Raw"):
                csv= pd.read_csv("Training_Raw_files_validated/Good_Raw"+ file)
                count =0
                for columns in csv:
                    if (len(csv[columns])- csv[columns].count())==len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count ==0:
                    csv.rename(columns ={"Unnamed: 0":"creditCardFraud"},inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw"+file,index=None , header=True)
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred:: %s" % e)
            f.close()
            raise e
        f.close()
