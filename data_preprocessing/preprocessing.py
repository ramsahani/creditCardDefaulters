import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler


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

    def separate_label_feature(self, data,label_column_name):
        """
        This method separates the feature and a Label columns
        :param label_column_name:
        :return:
        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()


    def impute_missing_values(self, data, cols_with_missing_values):
        """
         This method replaces all the missing values in DataFrame using KNN Imputer
        :param data:
        :param cols_with_missing_values:
        :return: A DataFrame which has all the missing values imputed.
        """

        self.logger_object.log(self.file_object, 'Entered the impute_missing_values method of Preprocessor class.')
        self.data = data
        self.cols_with_missing_values = cols_with_missing_values
        try:
            self.imputer = SimpleImputer(strategy="most_frequent")
            for col in self.cols_with_missing_values:
                self.data[col] = self.imputer.fit_transform(self.data[col])
            self.logger_object.log(self.file_object, "Imputing missing values Successful.Exited impute_missing_values method of Preprocessor class. ")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in impute_missing_values method of Preprocessor class. Exception message: %s' % e)

            self.logger_object.log(self.file_object, 'Imputing missing values failed.Exited the impute_missing_values method of Preprocessor class.')
            raise Exception()
    def scale_numerical_columns(self,data):
        """
             This method scales the numerical values using the Standard Scaler.
        :param data:
        :return: A DataFrame with scaled data
        """

        self.logger_object.log(self.file_object,
                               'Entered the scale_numerical_columns method of the Preprocessor class')

        self.data=data

        try:
            self.num_df = self.data.select_dtypes(include=['int64']).copy()
            self.scaler = StandardScaler()
            self.scaled_data = self.scaler.fit_transform(self.num_df)
            self.scaled_num_df = pd.DataFrame(data=self.scaled_data, columns=self.num_df.columns)


            self.logger_object.log(self.file_object, 'scaling for numerical values successful. Exited the scale_numerical_columns method of the Preprocessor class')
            return self.scaled_num_df

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in scale_numerical_columns method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'scaling for numerical columns Failed. Exited the scale_numerical_columns method of the Preprocessor class')
            raise Exception()




    def encode_categorical_columns(self, data):
        """
        This method encodes the categorical values to numerical values.
        :param data:
        :return:only the columns with categorical values converted to numerical values

        """

        self.logger_object.log(self.file_object, "Entered the encode_categorical_columns method of Preprocessor class.")
        try:
            self.cat_df = data.select_dtype(include = ['object']).copy()
            # Using the dummy encoding to encode the categorical columns to numerical ones
            for col in self.cat_df.columns:
                self.cat_df = pd.get_dummies(self.cat_df, columns=[col],prefix=[col],drop_first=True)

            self.logger_object.log(self.file_object, 'Encoding for categorical values successful.Exited the encode_categorical_columns method of Preprocessor class')
            return self.cat_df
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in encode_categorical_columns method of the Preprocessor class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'encoding for categorical columns Failed. Exited the encode_categorical_columns method of the Preprocessor class')
            raise Exception()

    # def handle_imbalanced_dataset(self, x, y):
    #     """
    #     This method handles the imbalanced data set to make it balanced one.
    #
    #     :param x:
    #     :param y:
    #     :return: new balanced feature and target columns
    #     """
    #
    #     self.logger_object.log(self.file_object ,'Entered the handle_imbalanced_dataset method of Preprocessor .')
    #     try:
    #         self.rdsmple = RandomOverSampler()
    #         self.x_sampled,self.y_sampled  = self.rdsmple.fit_sample(x,y)
    #         self.logger_object.log(self.file_object,
    #                                'dataset balancing successful. Exited the handle_imbalanced_dataset method of the Preprocessor class')
    #         return self.x_sampled,self.y_sampled
    #
    #     except Exception as e:
    #         self.logger_object.log(self.file_object,
    #                                'Exception occured in handle_imbalanced_dataset method of the Preprocessor class. Exception message:  ' + str(
    #                                    e))
    #         self.logger_object.log(self.file_object,
    #                                'dataset balancing Failed. Exited the handle_imbalanced_dataset method of the Preprocessor class')
    #         raise Exception()
