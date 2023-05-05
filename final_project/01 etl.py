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
# MAGIC ## Ignore all above

# COMMAND ----------

bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+ '202111_citibike_tripdata.csv')
bike_schema = bikedf.schema

weatherdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH+'NYC_Weather_Data.csv')
weather_schema = weatherdf.schema

# COMMAND ----------

rawbike_df = (spark.readStream
                    .option('header', 'true')
                    .schema(bike_schema)
                    .csv(BIKE_TRIP_DATA_PATH))

rawweather_df = (spark.readStream
                    .option('header', 'true')
                    .schema(weather_schema)
                    .csv(NYC_WEATHER_FILE_PATH))


# COMMAND ----------

query1 = (rawbike_df.writeStream
                    .outputMode('append')
                    .format('delta')
                    .option('checkpointLocation',
                    f'{GROUP_DATA_PATH}/bike_checkpoint')
                    .option('path', f'{GROUP_DATA_PATH}/bike_hist')
                    .trigger(once=True)
                    .start()
)

query2 = (rawweather_df.writeStream
                    .outputMode('append')
                    .format('delta')
                    .option('checkpointLocation',
                    f'{GROUP_DATA_PATH}/weather_checkpoint')
                    .option('path', f'{GROUP_DATA_PATH}/weather_hist')
                    .trigger(once=True)
                    .start()
)

# COMMAND ----------

query1.awaitTermination()

query2.awaitTermination()

display(spark.read.format('delta').load(f'{GROUP_DATA_PATH}/bike_hist').count())
rawbdf = spark.read.format('delta').load(f'{GROUP_DATA_PATH}/bike_hist')
rawbdf.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'Bronze_Bike_Data')


# COMMAND ----------

display(spark.read.format('delta').load(f'{GROUP_DATA_PATH}/weather_hist').count())
rawwdf = spark.read.format('delta').load(f'{GROUP_DATA_PATH}/weather_hist')
rawwdf.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'Bronze_Weather_Data')

from pyspark.sql.functions import col, when, lag, asc, to_timestamp, min, from_unixtime, date_format, to_date, to_timestamp

filteredb_df = rawbdf.filter((rawbdf.start_station_name == 'W 31 St & 7 Ave') | (rawbdf.end_station_name == 'W 31 St & 7 Ave'))
filteredb_df = filteredb_df.withColumn('relevant_time', when(col('start_station_name') == 'W 31 St & 7 Ave', to_timestamp(col('started_at'))) \
                 .when(col('end_station_name') == 'W 31 St & 7 Ave', to_timestamp(col('ended_at'))))
filteredb_df = filteredb_df.orderBy(asc(col('relevant_time')))
filteredw_df = rawwdf.withColumn('date_time', to_timestamp(from_unixtime(col('dt') + col('timezone_offset')), 'yyyy-MM-dd HH:mm:ss'))
filteredw_df = filteredw_df.orderBy(asc(col('date_time')))


# COMMAND ----------

import pandas as pd
import numpy as np

df1 = filteredb_df.toPandas()
df2 = filteredw_df.toPandas()

merged_df = pd.merge_asof(df1, df2, left_on = 'relevant_time', right_on='date_time', direction = 'nearest')

merged_df['left'] = 0
merged_df.loc[merged_df['start_station_name'] == 'W 31 St & 7 Ave', 'left'] = 1
merged_df['arrived'] = 0
merged_df.loc[merged_df['end_station_name'] == 'W 31 St & 7 Ave', 'arrived'] = 1
merged_df['relevant_time_plus_1h'] = merged_df['relevant_time'] + pd.Timedelta(hours=1)

merged_df['rolling_left'] = merged_df.set_index('relevant_time')['left'].rolling('1h').sum().values
merged_df['rolling_arrived'] = merged_df.set_index('relevant_time')['arrived'].rolling('1h').sum().values

merged_df['left_within_1h'] = merged_df.set_index('relevant_time_plus_1h')['rolling_left'].shift(1).values
merged_df['arrived_within_1h'] = merged_df.set_index('relevant_time_plus_1h')['rolling_arrived'].shift(1).values

merged_df['net_change'] = merged_df['arrived_within_1h'] - merged_df['left_within_1h']

merged_df = merged_df.drop(['relevant_time_plus_1h', 'rolling_left', 'rolling_arrived', 'arrived_within_1h', 'left_within_1h'], axis=1)

total_df = spark.createDataFrame(merged_df)

total_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GROUP_DATA_PATH + 'Silver_Data')


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
