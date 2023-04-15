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

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col


# Show the distinct rideable types
print("Distinct rideable types:")
bdf.select(col("rideable_type")).distinct().show()

# Show the distinct start station names
print("Distinct start station names:")
bdf.select(col("start_station_name")).distinct().show()

# Show the distinct end station names
print("Distinct end station names:")
bdf.select(col("end_station_name")).distinct().show()

# Show the number of rows by member/casual type
print("Number of rows by member/casual type:")
bdf.groupBy(col("member_casual")).count().show()

# Convert to datetime
bdf = bdf.withColumn("started_at_ts", unix_timestamp(col("started_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
bdf = bdf.withColumn("ended_at_ts", unix_timestamp(col("ended_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
bdf = bdf.withColumn("ride_duration_min", (col("ended_at_ts") - col("started_at_ts")) / 60)
# Show the average ride duration by member/casual type
print("Average ride duration by member/casual type:")
bdf.groupBy(col("member_casual")) \
   .agg({"ride_duration_min":"avg"}) \
   .withColumnRenamed("avg(ride_duration_min)", "average_ride_duration") \
   .show()


# COMMAND ----------

from pyspark.sql.functions import count
# Calculate the number of rides starting from each station
start_counts = bdf.groupBy('start_station_name').agg(count('*').alias('count'))

# Calculate the number of rides ending at each station
end_counts = bdf.groupBy('end_station_name').agg(count('*').alias('count'))

# Plot a bar chart of the results
display(start_counts.orderBy('count', ascending=False).limit(10))
display(end_counts.orderBy('count', ascending=False).limit(10))

# COMMAND ----------

from pyspark.sql.functions import count

# Calculate the distribution of rideable types
rideable_counts = bdf.groupBy('rideable_type').agg(count('*').alias('count'))

# Plot a bar chart of the results
display(rideable_counts)


# COMMAND ----------

import matplotlib.pyplot as plt

# Plot a histogram of ride durations
fig, ax = plt.subplots()
ride_duration_min_array = bdf.select('ride_duration_min').filter(col('ride_duration_min') < 100).rdd.flatMap(lambda x: x).collect()
ax.hist(ride_duration_min_array, bins=20, range=(0, 100))
plt.xlabel('Duration (minutes)')
plt.ylabel('Count')
plt.show()


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
