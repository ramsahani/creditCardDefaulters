from wsgiref import simple_server
from flask import Flask, request
from flask import Response
import os
from flask_cors import CORS, cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
#import flask_monitoringdashboard as dashboard
from predictFromModel import prediction
from application_logging import logger

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')



path = 'Prediction_Batch_Files'
#
# pred_val = pred_validation(path) #object initialization
# #
# pred_val.prediction_validation() #calling the prediction_validation function
#
pred = prediction(path) #object initialization
#
#     # predicting for dataset present in database
path ,some= pred.predictionFromModel()
print("Prediction File created at %s!!!" % path)
print(some)
#
# except ValueError:
#     print("Error Occurred! %s" %ValueError)
# except KeyError:
#     print("Error Occurred! %s" %KeyError)
# except Exception as e:
#     print("Error Occurred! %s" %e)
#





# path = 'Training_Batch_Files'
# #
# trainModelObj = trainModel() #object initialization
# trainModelObj.trainingModel() #training the model for the files in the table
# import  pandas as pd
#
# data = pd.read_csv('Training_FileFromDB/InputFile.csv')
# print(data.head())


























