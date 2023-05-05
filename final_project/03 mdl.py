# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

# COMMAND ----------

modeldf=spark.read.format('delta').option("header","True").option("inferSchema","True").load(GROUP_DATA_PATH + 'EDA_total')
display(modeldf)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col, hour, mean, max, count, to_timestamp, date_format, concat, lit
in_df = modeldf.filter(col('start_station_name') == 'W 31 St & 7 Ave')

in_df = (in_df
  .withColumn("relevant_date", col("relevant_time").cast("date"))
  .withColumn("relevant_hour", hour(col("relevant_time"))))

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
in_df = in_df.groupBy('relevant_date', 'relevant_hour').agg(
    mean('temp'),
    mean('feels_like'),
    max('rain_1h'),
    count('relevant_hour').alias('y')).withColumn('ds', to_timestamp(concat("relevant_date","relevant_hour"),"yyyy-MM-ddHH"))

#plt.plot(in_df)
display(in_df)

# COMMAND ----------

#!pip install pyramid
#import pyramid as pm
import matplotlib.pyplot as plt
import datetime
import matplotlib.dates as md
import numpy as np

history = in_df.select('ds', 'y','relevant_hour', 'avg(temp)')

history_pd = history.toPandas()
history_pd = history_pd.sort_values(by='ds', ascending=True)
print(history_pd)

#times = [datetime.datetime.fromtimestamp(i.astype('uint64') / 1e6).astype('uint32') for i in history_pd['ds']]

fig, ax = plt.subplots(figsize=(30,10))
plt.scatter(history_pd['ds'],history_pd['y'])



ax.set_xlim((np.datetime64('2023-01-01 00:00'), np.datetime64('2023-01-04 23:59')))

plt.show()

# COMMAND ----------

from sklearn.model_selection import train_test_split

train, valid = train_test_split(history_pd, test_size=0.2, shuffle=False)

xt, yt = train.drop(columns=['y']), train['y']
xv, yv = valid.drop(columns=['y']), valid['y']

# COMMAND ----------

import statsmodels.api as sm
model = sm.tsa.ARIMA(endog=yt, order=(1,1,1))

# COMMAND ----------

out_df = 

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
