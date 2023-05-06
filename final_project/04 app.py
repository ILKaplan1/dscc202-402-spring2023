# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

# 1. Load the required libraries and set up the environment
import datetime
import pandas as pd
import numpy as np
import plotly.express as px
import mlflow
from mlflow import get_experiment_by_name, get_run
from pyspark.sql.functions import col

# COMMAND ----------

# 2. Retrieve the current timestamp, production model version, and staging model version
now = datetime.datetime.now()
print("Current timestamp:", now)

# COMMAND ----------

model_name = "G08_model"

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

stage = "Staging"

# Create an instance of MlflowClient
client = MlflowClient()

# Get the model version details
model_version_details = client.get_latest_versions(name=model_name, stages=[stage])[0]

# Print the version of the model
print(f"The latest version of the '{model_name}' model in the '{stage}' stage is: {model_version_details.version}")


# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

model_name = "G08_model"
stage = "Production"

# Create an instance of MlflowClient
client = MlflowClient()

# Get the model versions for the specified model and stage
model_versions = client.search_model_versions(f"name='{model_name}'")

# Filter the versions to find the current production version
current_production_versions = [version for version in model_versions if version.current_stage == stage]

# Sort the versions by creation time in descending order
sorted_versions = sorted(current_production_versions, key=lambda v: v.creation_timestamp, reverse=True)

if sorted_versions:
    # Get the latest version
    current_production_version = sorted_versions[0]

    # Print the version of the model
    print(f"The current production version of the '{model_name}' model is: {current_production_version.version}")
else:
    print(f"No production version found for the '{model_name}' model.")


# COMMAND ----------

station_name = 'W 31 St & 7 Ave'
print("Our station name is:", station_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ![test image](https://i.ibb.co/NVsdMB6/figure.png)
# MAGIC
# MAGIC
# MAGIC The circle with the bolded black is our station

# COMMAND ----------

weather = spark.read.format('delta').load("dbfs:/FileStore/tables/bronze_nyc_weather.delta")

# COMMAND ----------

from pyspark.sql.functions import desc
from datetime import datetime
from pyspark.sql.functions import col, lit

# Sort the DataFrame by started_at column in descending order
sorted_weather = weather.orderBy(desc('time'))

# Get the current datetime
current_time = datetime.now()
current_time = current_time.replace(minute=0, second=0)
formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

# Display the sorted DataFrame
display(sorted_weather)
matching_row = sorted_weather.filter(col('time') == lit(formatted_time)).first()

# Get the values of 'temp' and 'rain.1h' from the matching row
temp_value = matching_row['temp']
rain_value = matching_row['rain.1h']

# Print the values
print("Temp value:", temp_value)
print("Rain value:", rain_value)
print(matching_row['time'])
print("Note: 'precip' as specified in the instructions did not exist, therefore we chose to use the past hour rain value")

# COMMAND ----------

station = spark.read.format('delta').load("dbfs:/FileStore/tables/bronze_station_status.delta")	

# COMMAND ----------

info = spark.read.format('delta').load("dbfs:/FileStore/tables/bronze_station_info.delta")	
    

# COMMAND ----------

capacity = info.filter(col('name') == lit('W 31 St & 7 Ave')).first()
print("Total capacity is:", capacity['capacity'])

# COMMAND ----------

display(station)

# COMMAND ----------

from pyspark.sql import functions as F

station = station.filter(col('station_id') == lit('66dbe4db-0aca-11e7-82f6-3863bb44ef7c'))
# Convert the last_reported column to datetime format
station = station.withColumn('last_reported', F.from_unixtime(F.col('last_reported')))
sorted_station = station.orderBy(F.col('last_reported').desc())
# Show the updated DataFrame
display(sorted_station)


# COMMAND ----------


# Select the desired columns
selected_cols = ['num_docks_available', 'num_ebikes_available', 'num_docks_disabled', 'num_bikes_disabled', 'num_bikes_available', 'last_reported']
selected_df = sorted_station.select(selected_cols).first()

# display(selected_df)
print("Number of docks available:", selected_df['num_docks_available'] - selected_df['num_docks_disabled'])
print("Number of bikes available:", selected_df['num_ebikes_available'] - selected_df['num_bikes_disabled'] + selected_df['num_bikes_available']) 


# COMMAND ----------

import mlflow.pyfunc
# Get the model version details
model_version_details = client.get_latest_versions(name=model_name, stages=[stage])[0]

# Load the model for predicting values
model = mlflow.pyfunc.load_model(model_uri=model_version_details.source)


# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/FileStore/tables/G08/' + 'Gold_Data')
display(df)

# COMMAND ----------

if (selected_df['num_docks_available'] - selected_df['num_docks_disabled']) == 0:
    print("The bike station is completely empty.")
elif (selected_df['num_ebikes_available'] - selected_df['num_bikes_disabled'] + selected_df['num_bikes_available']) == capacity['capacity']:
    print("The bike station is completely full.")
else:
    print("The bike station is neither completely empty nor completely full.")


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
