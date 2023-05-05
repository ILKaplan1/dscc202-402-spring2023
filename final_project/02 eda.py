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

wdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH+'NYC_Weather_Data.csv')
display(wdf)

# COMMAND ----------

bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(bdf)

# COMMAND ----------

mdf = spark.read.format('delta').load(GROUP_DATA_PATH + 'Silver_Data')

# COMMAND ----------

from pyspark.sql.functions import month, year

monthly_trips = mdf.groupBy(year("relevant_time").alias("year"), month("started_at").alias("month")).count()


# COMMAND ----------

import pandas as pd
import plotly.graph_objs as go
from pyspark.sql.functions import concat_ws, lit


monthly_trips = monthly_trips.withColumn('year', monthly_trips['year'].cast('int'))
monthly_trips = monthly_trips.withColumn('month', monthly_trips['month'].cast('int'))
monthly_trips = monthly_trips.withColumn('date', concat_ws('-', monthly_trips['year'], monthly_trips['month'], lit(1)))

monthly_trips = monthly_trips.select('date', 'count').toPandas()

#scatterplot
fig = go.Figure(
    go.Scatter(
        x=pd.to_datetime(monthly_trips['date']),
        y=monthly_trips['count'],
        mode='markers'
    )
)


fig.update_layout(
    xaxis_title='Date',
    yaxis_title='Number of trips',
    title='Monthly Trip Trends'
)

fig.show()


# COMMAND ----------

from pyspark.sql.functions import hour, count, avg
import plotly.graph_objs as go

hourly_trips = mdf.groupBy(hour("relevant_time").alias("hour")).agg(count("*").alias("num_trips"))
hourly_trips = hourly_trips.withColumn("hour", hourly_trips["hour"].cast("int"))
hourly_trips = hourly_trips.groupBy("hour").agg(avg("num_trips").alias("avg_count"))


hourly_trips_pd = hourly_trips.toPandas()

hour_of_day = hourly_trips_pd['hour']

#barchart
fig = go.Figure(
    go.Bar(
        x=hour_of_day,
        y=hourly_trips_pd['avg_count'],
    )
)

fig.update_layout(
    xaxis_title='Hour of Day',
    yaxis_title='Total Number of Trips',
    title='Hourly Trip Trends'
)

fig.show()


# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import month, hour, count, sum

mdf_month_hour = mdf.withColumn("month", month("relevant_time")).withColumn("hour", hour("relevant_time"))
trips_count = mdf_month_hour.groupBy("month", "hour", "relevant_time").agg(count("*").alias("num_trips"))
hourly_trips = trips_count.groupBy("month", "hour").agg(sum("num_trips").alias("total_trips"))

hourly_trips_pd = hourly_trips.toPandas()

#dictionary to map month numbers to month names
month_names = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

hourly_trips_pd['month'] = hourly_trips_pd['month'].map(month_names)

hourly_trips_pd['month_num'] = hourly_trips_pd['month'].map({v: k for k, v in month_names.items()})

hourly_trips_pd.sort_values('month_num', inplace=True)

hourly_trips_pd.drop(columns=['month_num'], inplace=True)

#lineplot
plt.figure(figsize=(12, 6))
sns.lineplot(data=hourly_trips_pd, x='hour', y='total_trips', hue='month', palette="Paired")

plt.xlabel('Hour of Day')
plt.ylabel('Total Number of Trips')
plt.title('Hourly Trip Trends by Month')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format, count, col, mean, hour, avg, max

#list of common holidays
common_holidays = ["01-01", "02-14", "03-17", "07-04", "08-31", "11-11", "12-25"]

filtered_mdf = mdf.withColumn("is_common_holiday", date_format("started_at", "MM-dd").isin(common_holidays))

daily_trips = filtered_mdf.groupBy("is_common_holiday", date_format("started_at", "MM-dd").alias("date")).agg(count("*").alias("num_trips"))

avg_daily_trips = daily_trips.groupBy("is_common_holiday").agg(mean(col("num_trips")).alias("avg_daily_trips"))

filtered_mdf = filtered_mdf.withColumn("trip_duration_minutes", (col("ended_at").cast("long") - col("started_at").cast("long")) / 60)

avg_trip_duration = filtered_mdf.groupBy("is_common_holiday").agg(avg(col("trip_duration_minutes")).alias("avg_trip_duration"))

#peak hours for holidays and non-holidays
hourly_trips = filtered_mdf.withColumn("trip_hour", hour("started_at")).groupBy("is_common_holiday", "trip_hour").agg(count("*").alias("num_trips"))
max_hourly_trips = hourly_trips.groupBy("is_common_holiday").agg(max("num_trips").alias("max_trips"))
peak_hours = max_hourly_trips.join(hourly_trips, on=["is_common_holiday"], how="inner").filter(col("max_trips") == col("num_trips")).select("is_common_holiday", "trip_hour")

avg_daily_trips_pd = avg_daily_trips.toPandas()
avg_trip_duration_pd = avg_trip_duration.toPandas()
peak_hours_pd = peak_hours.toPandas()

#barchart
fig = go.Figure(
    go.Bar(
        x=["Non-Holiday", "Common Holiday"],
        y=avg_daily_trips_pd['avg_daily_trips'],
    )
)

fig.update_layout(
    xaxis_title='Day Type',
    yaxis_title='Average Number of Trips per Day',
    title='Daily Trip Trends: Common Holidays vs. Non-Holidays'
)

fig.show()

#average trip duration and peak hours results
print("Average Trip Duration (in minutes):")
print(avg_trip_duration_pd)
print("\nPeak Hours:")
print(peak_hours_pd)


# COMMAND ----------

from pyspark.sql.functions import when, col
from pyspark.sql.functions import hour, count, avg
import seaborn as sns
import matplotlib.pyplot as plt

mdf = mdf.withColumn("weather_category", col("main"))

mdf_hour_weather = mdf.withColumn("hour", hour("relevant_time"))
hourly_weather_trips = mdf_hour_weather.groupBy("hour", "weather_category").agg(count("*").alias("num_trips"))

hourly_avg_weather_trips = hourly_weather_trips.groupBy("hour", "weather_category").agg(avg("num_trips").alias("avg_trips"))
hourly_avg_weather_trips_pd = hourly_avg_weather_trips.toPandas()

plt.figure(figsize=(16, 8))
sns.lineplot(data=hourly_avg_weather_trips_pd, x='hour', y='avg_trips', hue='weather_category', palette="deep")

plt.xlabel('Hour of Day')
plt.ylabel('Average Number of Trips')
plt.title('Hourly Trip Trends by Weather Condition')
plt.legend(title="Weather Condition")

plt.show()


# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, col


#distinct rideable types
print("Distinct rideable types:")
bdf.select(col("rideable_type")).distinct().show()

#distinct start station names
print("Distinct start station names:")
bdf.select(col("start_station_name")).distinct().show()

#distinct end station names
print("Distinct end station names:")
bdf.select(col("end_station_name")).distinct().show()

#number of rows by member/casual type
print("Number of rows by member/casual type:")
bdf.groupBy(col("member_casual")).count().show()

bdf = bdf.withColumn("started_at_ts", unix_timestamp(col("started_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
bdf = bdf.withColumn("ended_at_ts", unix_timestamp(col("ended_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
bdf = bdf.withColumn("ride_duration_min", (col("ended_at_ts") - col("started_at_ts")) / 60)
#average ride duration by member/casual type
print("Average ride duration by member/casual type:")
bdf.groupBy(col("member_casual")) \
   .agg({"ride_duration_min":"avg"}) \
   .withColumnRenamed("avg(ride_duration_min)", "average_ride_duration") \
   .show()


# COMMAND ----------

from pyspark.sql.functions import count
#number of rides starting from each station
start_counts = bdf.groupBy('start_station_name').agg(count('*').alias('count'))

# Calculate the number of rides ending at each station
end_counts = bdf.groupBy('end_station_name').agg(count('*').alias('count'))

# Plot a bar chart of the results
display(start_counts.orderBy('count', ascending=False).limit(10))
display(end_counts.orderBy('count', ascending=False).limit(10))

# COMMAND ----------

from pyspark.sql.functions import count

#distribution of rideable types
rideable_counts = bdf.groupBy('rideable_type').agg(count('*').alias('count'))

#barchart
display(rideable_counts)


# COMMAND ----------

import matplotlib.pyplot as plt

#histogram of ride durations
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
