# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)


# COMMAND ----------

#xyz = spark.read.format('delta').load(GROUP_DATA_PATH + 'EDA_data')
#xyz = xyz.dropDuplicates(['ride_id'])
#xyz.drop('start_end', 'num_bikes', 'total', 'num_e_bikes')
#xyz.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'EDA_data_bike')

# COMMAND ----------

#display(bdf)
#bdf.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'EDA_data_1')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to filter and add columns to historical data df

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import col, when, lag, asc, to_timestamp, min
# MAGIC from pyspark.sql.window import Window
# MAGIC 
# MAGIC def df_formatter(file):
# MAGIC     #bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
# MAGIC     bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(file)
# MAGIC     bdf = bikedf.filter((bikedf.start_station_name == 'W 31 St & 7 Ave') | (bikedf.end_station_name == 'W 31 St & 7 Ave'))
# MAGIC     bdf = bdf.withColumn('start_end', when(col('start_station_name') == 'W 31 St & 7 Ave', -1) \
# MAGIC                  .when(col('end_station_name') == 'W 31 St & 7 Ave', 1))
# MAGIC     bdf = bdf.withColumn('relevant_time', when(col('start_station_name') == 'W 31 St & 7 Ave', col('started_at')) \
# MAGIC                  .when(col('end_station_name') == 'W 31 St & 7 Ave', col('ended_at')))
# MAGIC     bdf = bdf.withColumn('classic_bike_column', when(bdf['rideable_type'] == 'classic_bike', bdf['start_end']).otherwise(0))
# MAGIC     bdf = bdf.withColumn('electric_bike_column', when(bdf['rideable_type'] == 'electric_bike', bdf['start_end']).otherwise(0))
# MAGIC     bdf = bdf.orderBy(asc(col('relevant_time')))
# MAGIC 
# MAGIC     df = bdf.toPandas()
# MAGIC 
# MAGIC     df['num_bikes'] = df['classic_bike_column'].cumsum()
# MAGIC     df['num_e_bikes'] = df['electric_bike_column'].cumsum()
# MAGIC 
# MAGIC     bdf = spark.createDataFrame(df)
# MAGIC     bdf = bdf.drop('classic_bike_column')
# MAGIC     bdf = bdf.drop('electric_bike_column')
# MAGIC     bdf = bdf.withColumn('total', col('num_bikes') + col('num_e_bikes'))
# MAGIC     
# MAGIC     return bdf

# COMMAND ----------

def padder(x):
    if x < 10:
        return '0' + str(x)
    else:
        return str(x)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregating all historic data into one df

# COMMAND ----------

# MAGIC %md
# MAGIC df = df_formatter(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
# MAGIC 
# MAGIC for i in dbutils.fs.ls(BIKE_TRIP_DATA_PATH)[1:-1]:
# MAGIC         df = df.union(df_formatter(i[0]))   
# MAGIC 
# MAGIC display(df)
# MAGIC df.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'EDA_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add weather data and aggregate

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col, date_format, to_date, to_timestamp, asc

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH+'NYC_Weather_Data.csv')
bdf = spark.read.format('delta').load(GROUP_DATA_PATH + 'EDA_data_bike')
bdf = bdf.withColumn('date_time', col('relevant_time'))
bdf = bdf.orderBy(asc(col('date_time')))
wdf = wdf.withColumn('date_time', to_timestamp(from_unixtime(col('dt') + col('timezone_offset')), 'yyyy-MM-dd HH:mm:ss'))
wdf = wdf.orderBy(asc(col('date_time')))

# COMMAND ----------

import pandas as pd
import numpy as np

df1 = bdf.toPandas()
df2 = wdf.toPandas()

merged_dataframe = pd.merge_asof(df1, df2, on='date_time', direction = 'nearest')
display(merged_dataframe)
print(len(df1))
print(len(merged_dataframe))
total_df = spark.createDataFrame(merged_dataframe)
total_df = total_df.drop('date_time', 'dt', 'start_end', 'num_bikes', 'num_e_bikes', 'total', 'timezone_offset', 'timezone')
total_df.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'EDA_total')

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
