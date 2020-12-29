from os import listdir
import pandas
from application_logging.logger import App_Logger


class dataTransform:
    """
    This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

    """
    def __init__(self):
        self.goodDataPath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def replaceMissingWithNull(self):
        """
                This method replaces the missing values in columns with "NULL" to
                store in the table.
        :return: None
        """
        log_file = open("Prediction_Logs/dataTransformLog.txt",'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pandas.read_csv(self.goodDataPath + "/" + file)
                data.fillna("NULL",inplace=True)
                data.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                self.logger.log(log_file, "%s: Quotes added successfully !!!" % file)


        except Exception as e:
            self.logger.log(log_file, "Data Transform failed because:: %s" % file)
            log_file.close()

        log_file.close()