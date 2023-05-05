# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

mdf = spark.read.format('delta').load(GROUP_DATA_PATH + 'Silver_Data')
display(mdf)

# COMMAND ----------

from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.spark
import pandas as pd
import numpy as np
from hyperopt import hp
from hyperopt import fmin, tpe, Trials
from hyperopt import SparkTrials
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

mdf = spark.read.format('delta').load("dbfs:/FileStore/tables/G08/" + 'Silver_Data')
display(mdf)

# COMMAND ----------

from pyspark.sql.functions import col

net_change_range = mdf.agg({"net_change": "max"}).collect()[0][0] - mdf.agg({"net_change": "min"}).collect()[0][0]
net_change_avg = mdf.agg({"net_change": "avg"}).collect()[0][0]

print(f'Net change range: {net_change_range}')
print(f'Net change average: {net_change_avg}')

# COMMAND ----------

mdf.printSchema()

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor

# def train_net_change_model(mdf):
#     mdf = mdf.dropna()

#     # Create feature vector
#     feature_cols = ['temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h']
#     assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
#     mdf = assembler.transform(mdf)

#     # Split data into training and test sets
#     (train_df, test_df) = mdf.randomSplit([0.8, 0.2])

#     # Train random forest regression model
#     rf = RandomForestRegressor(labelCol='net_change', featuresCol='features', numTrees=10)
#     pipeline = Pipeline(stages=[rf])
#     model = pipeline.fit(train_df)

#     # Evaluate model on test set
#     predictions = model.transform(test_df)
#     evaluator = RegressionEvaluator(labelCol='net_change', predictionCol='prediction', metricName='rmse')
#     rmse = evaluator.evaluate(predictions)

#     print('Root Mean Squared Error (RMSE) on test data = %g' % rmse)

#     return model
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
import mlflow
import mlflow.spark

def train_net_change_model(mdf):
    mlflow.spark.autolog()
    mdf = mdf.dropna()

    # Create feature vector
    feature_cols = ['temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    mdf = assembler.transform(mdf)

    # Split data into training and test sets
    (train_df, test_df) = mdf.randomSplit([0.8, 0.2])

    # Define the model
    rf = RandomForestRegressor(labelCol='net_change', featuresCol='features')

    # Define the parameter grid to search over
    param_grid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [10, 20, 30]) \
        .addGrid(rf.maxDepth, [5, 10, 15]) \
        .build()

    # Define the TrainValidationSplit object
    tvs = TrainValidationSplit(estimator=rf, estimatorParamMaps=param_grid, evaluator=RegressionEvaluator(labelCol='net_change', predictionCol='prediction', metricName='rmse'), trainRatio=0.8)

    # Start an MLflow experiment
    with mlflow.start_run(run_name = 'training'):

        # Fit the model on the training data
        model = tvs.fit(train_df)

        # Evaluate model on test set
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol='net_change', predictionCol='prediction', metricName='rmse')
        rmse = evaluator.evaluate(predictions)

        # Log model parameters and metrics to MLflow
        mlflow.log_param('num_trees', model.bestModel.getNumTrees)
        mlflow.log_param('max_depth', model.bestModel.getMaxDepth())
        mlflow.log_metric('rmse', rmse)

        # Log the trained model to MLflow
        mlflow.spark.log_model(model.bestModel, 'model')

        # Return the model
        return model


# COMMAND ----------

model_name = 'G08_model'

# COMMAND ----------

import mlflow
mlflow.set_experiment("/Repos/mkingsl6@u.rochester.edu/dscc202-402-spring2023/final_project/03 mdl")

# COMMAND ----------

# with mlflow.start_run():
#     # Train your model
#     net_change_model = train_net_change_model(mdf)
#     # Log the model with MLflow
#     mlflow.spark.log_model(net_change_model, model_name)
# mlflow.end_run()
model = train_net_change_model(mdf)

# COMMAND ----------

import mlflow
logged_model = 'runs:/cf7a1773bc3b400d834a14f450a4518e/model'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)

# COMMAND ----------

temp = mdf

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Imputer

# Select the input features from mdf
input_cols = ['temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'pop', 'snow_1h']

# Fill in missing values in the input_cols
imputer = Imputer(inputCols=input_cols, outputCols=[f"{col}_imputed" for col in input_cols])
filled_mdf = imputer.fit(mdf).transform(mdf)

# Use VectorAssembler to assemble the input features into a vector column
assembler = VectorAssembler(inputCols=[f"{col}_imputed" for col in input_cols], outputCol="features")
mdf_with_features = assembler.transform(filled_mdf)

# Use the loaded model to perform inference on the mdf_with_features DataFrame
predictions = loaded_model.transform(mdf_with_features)


# COMMAND ----------

predictions.select("prediction", 'ride_id').show()

# COMMAND ----------

from sklearn.metrics import r2_score

# COMMAND ----------

preds = predictions.select("prediction", 'ride_id')
true = mdf.select('net_change', 'ride_id')

combined = preds.join(true, on = 'ride_id')
combined.show()

# COMMAND ----------

from pyspark.sql.functions import col

def r2(df, actual_col, predicted_col):
    mean_actual = df.selectExpr(f"avg({actual_col})").collect()[0][0]
    ss_total = df.selectExpr(f"sum(pow(({actual_col} - {mean_actual}), 2))").collect()[0][0]
    ss_residual = df.selectExpr(f"sum(pow(({actual_col} - {predicted_col}), 2))").collect()[0][0]
    r2 = 1 - (ss_residual / ss_total)
    return r2


# COMMAND ----------

display(combined)

# COMMAND ----------

print("R2:", r2(combined, 'net_change', 'prediction'))

# COMMAND ----------



# COMMAND ----------

dbutils.fs.cp("runs:/<RUN_ID>/artifacts/<MODEL_NAME>", "/tmp/<MODEL_NAME>")

# Register the staging model
!databricks mlflow model create --run-id <RUN_ID> --model-name <MODEL_NAME> --staging

# Register the production model
!databricks mlflow model create --run-id <RUN_ID> --model-name <MODEL_NAME> --production


# COMMAND ----------

with mlflow.start_run():
    # Train your model
    net_change_model = train_net_change_model(mdf)
    
    # Log the model with MLflow and store the artifacts
    mlflow.spark.log_model(net_change_model, model_name, artifact_path="staging")
    mlflow.spark.log_model(net_change_model, model_name, artifact_path="production")


# COMMAND ----------

param_space = {
    'order': hp.choice('order', [(1, 1, 0), (2, 1, 0), (3, 1, 0)]),
    'seasonal_order': hp.choice('seasonal_order', [((1, 0, 0, 12)), ((2, 0, 0, 12)), ((3, 0, 0, 12))]),
    'enforce_stationarity': hp.choice('enforce_stationarity', [True, False]),
    'enforce_invertibility': hp.choice('enforce_invertibility', [True, False])
}


# COMMAND ----------

# Define the evaluation function for hyperopt
def eval_sarima(params):
    # Extract hyperparameters
    order = params['order']
    seasonal_order = params['seasonal_order']
    enforce_stationarity = params['enforce_stationarity']
    enforce_invertibility = params['enforce_invertibility']

    # Split data into train and test sets
    train_data, test_data = mdf.randomSplit([0.7, 0.3], seed=123)

    # Train the model
    model = SARIMAX(train_data.select('net_change').toPandas(),
                    order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=enforce_stationarity,
                    enforce_invertibility=enforce_invertibility)
    model_fit = model.fit()

    # Make predictions on test data
    pred = model_fit.forecast(steps=len(test_data))

    # Calculate MSE
    mse = mean_squared_error(test_data.select('net_change').toPandas(), pred)

    # Log the hyperparameters and evaluation metric
    with mlflow.start_run():
        mlflow.log_params(params)
        mlflow.log_metric('mse', mse)

    # Return MSE for hyperopt optimization
    return mse

# COMMAND ----------

# Use SparkTrials to scale hyperopt optimization
spark_trials = SparkTrials(parallelism=2)

# Optimize the model hyperparameters using hyperopt
with mlflow.start_run():
    best_params = fmin(fn=eval_sarima,
                       space=param_space,
                       algo=tpe.suggest,
                       max_evals=10,
                       trials=spark_trials)

    # Log the best hyperparameters
    mlflow.log_params(best_params)

    # Train the model using the best hyperparameters
    order = best_params['order']
    seasonal_order = best_params['seasonal_order']
    enforce_stationarity = best_params['enforce_stationarity']
    enforce_invertibility = best_params['enforce_invertibility']

    model = SARIMAX(mdf.select('net_change').toPandas(),
                    order=order, seasonal_order=seasonal_order,
                    enforce_stationarity=enforce_stationarity,
                    enforce_invertibility=enforce_invertibility)
    model_fit = model.fit()
    
    # Save the model as "G08_model"
    mlflow.spark.log_model(model_fit, "G08_model")


# COMMAND ----------



# COMMAND ----------

from sklearn.model_selection import TimeSeriesSplit

# COMMAND ----------

import statsmodels.api as sm
model = sm.tsa.ARIMA(endog=yt, order=(1,1,1))

# COMMAND ----------

out_df = 

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
