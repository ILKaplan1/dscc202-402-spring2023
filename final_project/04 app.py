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



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
