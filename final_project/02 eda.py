# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import col

bikedf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
bikedf = bikedf.orderBy(col('started_at'))
display(bikedf)

# COMMAND ----------



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


# Extract year and month columns
monthly_trips = monthly_trips.withColumn('year', monthly_trips['year'].cast('int'))
monthly_trips = monthly_trips.withColumn('month', monthly_trips['month'].cast('int'))
monthly_trips = monthly_trips.withColumn('date', concat_ws('-', monthly_trips['year'], monthly_trips['month'], lit(1)))

monthly_trips = monthly_trips.select('date', 'count').toPandas()

# Create a scatter plot
fig = go.Figure(
    go.Scatter(
        x=pd.to_datetime(monthly_trips['date']),
        y=monthly_trips['count'],
        mode='markers'
    )
)

# Set the axis labels and title
fig.update_layout(
    xaxis_title='Date',
    yaxis_title='Number of trips',
    title='Monthly Trip Trends'
)

# Display the plot
fig.show()


# COMMAND ----------

from pyspark.sql.functions import hour, count, avg
import plotly.graph_objs as go

hourly_trips = mdf.groupBy(hour("relevant_time").alias("hour")).agg(count("*").alias("num_trips"))
hourly_trips = hourly_trips.withColumn("hour", hourly_trips["hour"].cast("int"))
hourly_trips = hourly_trips.groupBy("hour").agg(avg("num_trips").alias("avg_count"))

# Convert the Spark DataFrame to a Pandas DataFrame
hourly_trips_pd = hourly_trips.toPandas()

# Extract the desired column as a Pandas Series
hour_of_day = hourly_trips_pd['hour']

# Create a bar chart
fig = go.Figure(
    go.Bar(
        x=hour_of_day,
        y=hourly_trips_pd['avg_count'],
    )
)

# Set the axis labels and title
fig.update_layout(
    xaxis_title='Hour of Day',
    yaxis_title='Total Number of Trips',
    title='Hourly Trip Trends'
)

# Display the plot
fig.show()


# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import month, hour, count, sum

# Create a new DataFrame with the month and hour of the trips
mdf_month_hour = mdf.withColumn("month", month("relevant_time")).withColumn("hour", hour("relevant_time"))

# Group trips by month, day, and hour, then count the number of trips
trips_count = mdf_month_hour.groupBy("month", "hour", "relevant_time").agg(count("*").alias("num_trips"))

# Group trips by month and hour, then sum the number of trips
hourly_trips = trips_count.groupBy("month", "hour").agg(sum("num_trips").alias("total_trips"))

# Convert the Spark DataFrame to a Pandas DataFrame
hourly_trips_pd = hourly_trips.toPandas()

# Create a dictionary to map month numbers to month names
month_names = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

# Replace the month numbers with month names
hourly_trips_pd['month'] = hourly_trips_pd['month'].map(month_names)

# Add a new column with month numbers
hourly_trips_pd['month_num'] = hourly_trips_pd['month'].map({v: k for k, v in month_names.items()})

# Sort the DataFrame by the 'month_num' column
hourly_trips_pd.sort_values('month_num', inplace=True)

# Drop the 'month_num' column as it's no longer needed
hourly_trips_pd.drop(columns=['month_num'], inplace=True)

# Create the line plot
plt.figure(figsize=(12, 6))
sns.lineplot(data=hourly_trips_pd, x='hour', y='total_trips', hue='month', palette="Paired")

# Set the axis labels and title
plt.xlabel('Hour of Day')
plt.ylabel('Total Number of Trips')
plt.title('Hourly Trip Trends by Month')

# Move the legend to the right
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

# Display the plot
plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format, count, col, mean, hour, avg, max

# Define list of common holidays
common_holidays = ["01-01", "02-14", "03-17", "07-04", "08-31", "11-11", "12-25"]

# Create a new column indicating whether the date is a common holiday or not
filtered_mdf = mdf.withColumn("is_common_holiday", date_format("started_at", "MM-dd").isin(common_holidays))

# Group trips by date and whether it's a common holiday or not
daily_trips = filtered_mdf.groupBy("is_common_holiday", date_format("started_at", "MM-dd").alias("date")).agg(count("*").alias("num_trips"))

# Calculate the average number of trips per day for each category
avg_daily_trips = daily_trips.groupBy("is_common_holiday").agg(mean(col("num_trips")).alias("avg_daily_trips"))

# Calculate trip duration in minutes
filtered_mdf = filtered_mdf.withColumn("trip_duration_minutes", (col("ended_at").cast("long") - col("started_at").cast("long")) / 60)

# Average trip duration for holidays and non-holidays
avg_trip_duration = filtered_mdf.groupBy("is_common_holiday").agg(avg(col("trip_duration_minutes")).alias("avg_trip_duration"))

# Calculate peak hours for holidays and non-holidays
hourly_trips = filtered_mdf.withColumn("trip_hour", hour("started_at")).groupBy("is_common_holiday", "trip_hour").agg(count("*").alias("num_trips"))
max_hourly_trips = hourly_trips.groupBy("is_common_holiday").agg(max("num_trips").alias("max_trips"))
peak_hours = max_hourly_trips.join(hourly_trips, on=["is_common_holiday"], how="inner").filter(col("max_trips") == col("num_trips")).select("is_common_holiday", "trip_hour")

# Convert the Spark DataFrames to Pandas DataFrames
avg_daily_trips_pd = avg_daily_trips.toPandas()
avg_trip_duration_pd = avg_trip_duration.toPandas()
peak_hours_pd = peak_hours.toPandas()

# Create a bar chart for average daily trips
fig = go.Figure(
    go.Bar(
        x=["Non-Holiday", "Common Holiday"],
        y=avg_daily_trips_pd['avg_daily_trips'],
    )
)

# Set the axis labels and title for the bar chart
fig.update_layout(
    xaxis_title='Day Type',
    yaxis_title='Average Number of Trips per Day',
    title='Daily Trip Trends: Common Holidays vs. Non-Holidays'
)

# Display the bar chart
fig.show()

# Display the average trip duration and peak hours results
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
