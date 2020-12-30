import pandas as pd


class Data_Getter:
    """
    This class shall be used for obtaining the data from the training.

    """

    def __init__(self, file_object, logger_object):
        self.training_file = 'Prediction_FileFromDB/InputFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):
        """
         This method reads the data from source.
        :return: a pandas DataFrame
        """
        self.logger_object.log(self.file_object, "Entered the get_data method of the Data_Getter class")
        try:
            self.data = pd.read_csv(self.training_file)  # reading the data file
            self.logger_object.log(self.file_object,
                                   "Data Load successful. Exited the get_data method of Data_Getter class ")
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   "Exception occurred in the get_data method of Data_Getter class.Exception message : %s " % e)
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful. Exited the get_data method of Data_Getter class.')
            raise Exception()