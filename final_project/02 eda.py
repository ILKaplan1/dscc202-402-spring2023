# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import col

bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
bikedf = bikedf.orderBy(col('started_at'))
display(bikedf)

# COMMAND ----------

df = spark.read.format('delta').load(GROUP_DATA_PATH + 'bdf')
display(df)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col, date_format, to_date
 
df = spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH)
df = df.withColumn('datetime_column', from_unixtime(col('last_reported')))
#display(df)
#filtered_df = df.filter(col('datetime_column') < '2021-11-01')
filtered_df = df.filter(col('station_id') ==  '66dbe4db-0aca-11e7-82f6-3863bb44ef7c')
##df_filtered = filtered_df.filter(col('datetime_column').cast('date') < '2021-11-01')
#display(filtered_df.withColumn('x', to_date(date_format(filtered_df['datetime_column'], 'yyyy-MM-dd'))))
#filtered_df = filtered_df.filter(to_date(date_format(filtered_df['datetime_column'], 'yyyy-MM-dd')) < '2021-11-01')
#filtered_df = filtered_df.filter(col('station_id') ==  '66dbe4db-0aca-11e7-82f6-3863bb44ef7c')
#display(filtered_df)
sorted_df = filtered_df.sort('datetime_column', ascending=True)
display(sorted_df)
last_num_bikes_available = sorted_df.select('num_bikes_available').first()[0]
last_num_ebikes_available = sorted_df.select('num_ebikes_available').first()[0]
print(last_num_bikes_available)
print(last_num_ebikes_available)

# COMMAND ----------

#display(sorted_df)

# COMMAND ----------

'''start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

display(spark.read.format('delta').load(BRONZE_STATION_INFO_PATH))
 
display(spark.read.format('delta').load(BRONZE_STATION_STATUS_PATH))
'''
'''
bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)
pdf = bdf.toPandas()

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH+'NYC_Weather_Data.csv')
display(wdf)
'''

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
