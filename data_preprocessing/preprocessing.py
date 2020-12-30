import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class Preprocessor:
    """
        This class shall be used to clean and transform the data before training.
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_unwanted_spaces(self, data):
        """
            This method removes the unwanted spaces from a pandas DataFrame.
        :param data:
        :return: a pandas DataFrame after removing the spaces.
        """

        try:
            self.df_without_spaces = self.data.apply(lambda x: x.str.strip() if x.dtype== 'object' else x )
            self.logger_object.log(self.file_object,"Unwanted spaces removal Successful.")
            return self.df_without_spaces

        except Exception as e:
            self.logger_object(self.file_object,"Exception occurred in remove_unwanted_spaces_method of Preprocessor class.Exception message : %s" % e)
            self.logger_object.log(self.file_object,'unwanted space removal Unsuccessful. Exited the unwanted_space_removal method of Preprocessor class.')
            raise Exception()


    def remove_columns(self, data,columns):
        """
        This method removes the given columns from a pandas DataFrame.
        :param data: A pandas DataFrame after removing the specified columns.
        :param columns:
        :return:
        """

        self.logger_object.log(self.file, 'Entered the remove_columns method of Preprocessor class.')
        self.data = data
        self.columns =columns
        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            self.logger_object.log(self.file_object,'Column removal Successful. Exited the remove_columns method of Preprocessor class.')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object, "Exception occurred in the remove_columns method of Preprocessor class.Exception message : %s" % e)
            self.logger_object.log(self.file_object, 'Column removal unsuccessful. Exited the remove_columns method of Preprocessor class')
            raise Exception()

    def is_null_present(self, data):
        """
        This method checks whether there are null values pandas present in the pandas DataFrame or not.
        :param data:
        :return: Returns True if null values present in DataFrame, False if not and
                 returns the list of columns for which null values are present.

        """

        self.logger_object.log(self.file_object, 'Entered is_null_present method of Preprocessor class.')
        self.null_present = False
        self.cols_with_missing_values = []
        self.cols = data.columns
        try:
            self.null_counts = data.isna().sum() #check for the count of null values per column
            for i in range(len(self.null_counts)):
                if self.null_counts[i] > 0:
                    self.null_present = True
                    self.cols_with_missing_values.apppend(self.cols[i])

            if(self.null_present): # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asanyarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object, 'Finding missing values is a success.Data written to null_values file. Exited the is_null method of Preprocessor class. ')
            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()
