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
        self.path = "Prediction_Database/"
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/GoodRaw"
        self.logger = App_Logger()


    def dataBaseConncetion(self, DatabaseName):
        """
        This method creates database of the given if database already exits then opens connection to Database.
        :param DatabaseName:
        :return: Connection to the DB
        """

        try:
            conn = _sqlite3.connect(self.path+DatabaseName+'.db')
            file = open('Prediction_Logs/DataBaseConnectionLog.txt','a+')
            self.logger.log(file, "Opened %s database successfully " % DatabaseName)
            file.close()

        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt",'a+')
            self.logger.log(file, "Error occurred while connecting to database %s " %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self, DatabaseName, column_names):
        """
        This method creates table in the given database which we will be used to insert the Good data after
        raw data validation.
        :param DatabaseName:
        :param column_names:
        :return:None
        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e



    def insertInotTableGoodData(self, Database):
        """
        This method inserts the Good data files from the Good Data folder into the table created in db.
        and if column type doesn't matches move it to the Bad Data folder.
        :param Database:
        :return: None
        """

        conn = self.dataBaseConncetion(Database)
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Prediction_Logs/DbInsertLog.txt",'a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/' + file, 'r') as f:
                    next(f)
                    reader = csv.reader(f, delimeter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute("INSERT INTO Good_Raw_Data value ({values})".format(values=list_ ))
                                self.logger.log(log_file," %s: File loaded successfully !! " % file)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise e


    def selectingDatafromtableintocsv(self,Database):
        """
          This method exports the data in GoodData table as a CSV file.
          above created.
        :param Database:
        :return:
        """

        self.fileFromDb= 'Prediction_FileFromDB/'
        self.fileName= 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt",'a+')
        try:
            conn = self.dataBaseConncetion(Database)
            sqlSelect = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()
            # get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV output directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            #Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w',newline=''),delimeter =',',lineterminator= '\r\n',quoting = csv.QUOTE_ALL,escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerow(results)

            self.logger.log(log_file, 'File  exported successfully !! ')

        except Exception as e:
            self.logger.log(log_file, "File exporting Failed : %s" % e)
            log_file.close()
            conn.close()
            raise e
