# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

import mlflow 
import mlflow.sklearn
import shutil

from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType

# COMMAND ----------

from sklearn.model_selection import TimeSeriesSplit

# COMMAND ----------

mdf = spark.read.format('delta').load('dbfs:/FileStore/tables/G08/' + 'Silver_Data')	

# COMMAND ----------

mdf = spark.read.format('delta').load(GROUP_DATA_PATH + 'Silver_Data')

# COMMAND ----------

mdf = mdf.withColumn('start_hour', hour('started_at'))

# COMMAND ----------

import json
# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
