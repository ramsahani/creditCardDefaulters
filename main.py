from flask import Flask, request, render_template
from flask import Response
from wsgiref import  simple_server
import os
from flask_cors import CORS , cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import  train_validation
from predictFromModel import prediction
import flask_monitoringdashboard as dashboard
import json

os.putenv('LANG','en_US.UTF-8')
os.putenv('LC_ALL','en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path= request.json['filepath']

            pred_val = pred_validation(path) # object initialization

            pred_val.prediction_validation() # calling the prediction_validation function

            pred = prediction(path) # object initialization

            # predicting for dataset present in database
            path = pred.predictionFromModel()
            return Response("Prediction File created at %s!!" % path)
        elif request.form is not None:
            path = request.form['filepath']

            pred_val = pred_validation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path)  # object initialization

            # predicting for dataset present in database
            path,json_predictions = pred.predictionFromModel()
            return Response("Prediction File created at %s!!!" +str(path) + 'and few of the predictions are ' + str(
                json.loads(json_predictions)))
    except ValueError:
        return Response("Error Occurred! %s" %ValueError)
    except KeyError:
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        return Response("Error Occurred! %s" %e)


@app.route("/train", methods = ['GET'])
@cross_origin()
def trainingRouteClient():
    try:
        if request.json['filepath'] is not None:
            path = request.json['filepath']
            train_valObj = train_validation(path) #object initialization

            train_valObj.train_validation() #calling the training_validation function

            trainModelObj = trainModel() # object_initialization
            trainModelObj.trainingModel()

    except ValueError:

        return Response("Error Occurred! %s " % ValueError)

    except KeyError:
        return Response("Error Occurred! %s " % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return RecursionError("Training successful!!")

# port = int(os.getenv("PORT",5001))
if __name__ == "__main__":
    host = '0.0.0.0'
    port = 5000

    httpd = simple_server.make_server(host, port, app)

    httpd.serve_forever()
