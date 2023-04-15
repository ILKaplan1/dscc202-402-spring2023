# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)


# COMMAND ----------

xyz = spark.read.format('delta').load(GROUP_DATA_PATH + 'EDA_data')
display(xyz)
print(xyz.count())
#abc = spark.read.format('delta').load(GROUP_DATA_PATH + 'bdf')
#print(abc.count())

# COMMAND ----------

xyz.agg(min("num_e_bike"), max("nume_e_bike")).show()
xyz.agg(min("num_bike"), max("nume_bike")).show()

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

'''
df = df_formatter(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
df.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'EDA_data_202111_citibike_tripdata.csv')

for i in dbutils.fs.ls(BIKE_TRIP_DATA_PATH)[1:]:
        df = df_formatter(i[0])
        df.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + i[0])
'''

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
# MAGIC bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
# MAGIC 
# MAGIC bdf = bikedf.filter((bikedf.start_station_name == 'W 31 St & 7 Ave') | (bikedf.end_station_name == 'W 31 St & 7 Ave'))
# MAGIC bdf = bdf.withColumn('start_end', when(col('start_station_name') == 'W 31 St & 7 Ave', -1) \
# MAGIC                  .when(col('end_station_name') == 'W 31 St & 7 Ave', 1))
# MAGIC bdf = bdf.withColumn('relevant_time', when(col('start_station_name') == 'W 31 St & 7 Ave', col('started_at')) \
# MAGIC                  .when(col('end_station_name') == 'W 31 St & 7 Ave', col('ended_at')))
# MAGIC bdf = bdf.withColumn('classic_bike_column', when(bdf['rideable_type'] == 'classic_bike', bdf['start_end']).otherwise(0))
# MAGIC bdf = bdf.withColumn('electric_bike_column', when(bdf['rideable_type'] == 'electric_bike', bdf['start_end']).otherwise(0))
# MAGIC bdf = bdf.orderBy(asc(col('relevant_time')))
# MAGIC 
# MAGIC df = bdf.toPandas()
# MAGIC 
# MAGIC df['num_bikes'] = df['classic_bike_column'].cumsum()
# MAGIC df['num_e_bikes'] = df['electric_bike_column'].cumsum()
# MAGIC 
# MAGIC bdf = spark.createDataFrame(df)
# MAGIC bdf = bdf.drop('classic_bike_column')
# MAGIC bdf = bdf.drop('electric_bike_column')
# MAGIC bdf = bdf.withColumn('total', col('num_bikes') + col('num_e_bikes'))
# MAGIC 
# MAGIC display(bdf)
# MAGIC #bdf.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'bdf')

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, when, lag, asc, to_timestamp, min
from pyspark.sql.window import Window


bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')

bdf = bikedf.filter((bikedf.start_station_name == 'W 31 St & 7 Ave') | (bikedf.end_station_name == 'W 31 St & 7 Ave'))
bdf = bdf.withColumn('start_end', when(col('start_station_name') == 'W 31 St & 7 Ave', -1) \
                 .when(col('end_station_name') == 'W 31 St & 7 Ave', 1))
bdf = bdf.withColumn('relevant_time', when(col('start_station_name') == 'W 31 St & 7 Ave', col('started_at')) \
                 .when(col('end_station_name') == 'W 31 St & 7 Ave', col('ended_at')))
bdf = bdf.withColumn('classic_bike_column', when(df['rideable_type'] == 'classic_bike', df['start_end']).otherwise(0))
bdf = bdf.withColumn('electric_bike_column', when(df['rideable_type'] == 'electric_bike', df['start_end']).otherwise(0))
bdf = bdf.orderBy(asc(col('relevant_time')))

df = bdf.toPandas()

df['num_bikes'] = df['classic_bike_column'].cumsum()
df['num_e_bikes'] = df['electric_bike_column'].cumsum()

bdf = spark.createDataFrame(df)
display(bdf)
'''
df_e = df
df = df.loc[df['rideable_type'] == 'classic_bike']
df_e = df_e.loc[df_e['rideable_type'] == 'electric_bike']
df = df.sort_values(by='relevant_time')
df['num_bikes'] = 0
df['num_e_bikes'] = 0
start_end_dict = {'start': -1, 'end': 1}
for i, row in df.iterrows():
    
    if i == 0:
        continue
        
    df.at[i, 'num_bikes'] = df.at[i-1, 'num_bikes']
    df.at[i, 'num_e_bikes'] = df.at[i-1, 'num_e_bikes']
    
    if row['rideable_type'] == 'classic_bike':
        df.at[i, 'num_bikes'] = df.at[i - 1, 'num_bikes'] + start_end_dict[row['start_end']]
    else:
        df.at[i, 'num_e_bikes'] = df.at[i - 1, 'num_e_bikes'] + start_end_dict[row['start_end']]
'''
 '''
    elif row['start_end'] == 'start':
        if row['rideable_type'] == 'classic_bike':
            prev_val = df.at[i-1, 'num_bikes']
            df.at[i, 'num_bikes'] = prev_val - 1
            #df.at[i, 'num_bikes'] = prev_val - 1
        else:
            prev_val = df.at[i-1, 'num_e_bikes']
            df.at[i, 'num_e_bikes'] = prev_val - 1
    elif row['start_end'] == 'end':
        if row['rideable_type'] == 'classic_bike':
            prev_val = df.at[i-1, 'num_bikes']
            df.at[i, 'num_bikes'] = prev_val + 1
        else:
            prev_val = df.at[i-1, 'num_e_bikes']
            df.at[i, 'num_e_bikes'] = prev_val + 1
'''

'''
window = Window.partitionBy().orderBy(asc('relevant_time'))

bdf = bdf.withColumn('num_bikes', when(col('start_end') == 'start', 1).otherwise(0))
bdf = bdf.withColumn('prev_num_bikes', lag(col('num_bikes')).over(window))
bdf = bdf.withColumn('num_bikes', when(col('start_end') == 'start', col('prev_num_bikes') - 1)
                   .when(col('start_end') == 'end', col('prev_num_bikes') + 1)
                   .otherwise(col('num_bikes')))
bdf = bdf.drop('prev_num_bikes')

#Below was figured out by running query on BRONZE_STATION_STATUS to find num_bikes_available right before first row in df

bdf = bdf.withColumn('num_bikes', when(bdf.num_bikes.isNull(), 0).otherwise(bdf.num_bikes))
bdf = bdf.withColumn('num_bikes', col('num_bikes') + 1)

bdf.write.format("delta").mode("overwrite").save(GROUP_DATA_PATH + 'bdf')
'''
#bdf = spark.createDataFrame(df)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
