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

mdf = spark.read.format('delta').load(GROUP_DATA_PATH + 'EDA_total')

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
    yaxis_title='Average Number of Trips',
    title='Hourly Trip Trends'
)

# Display the plot
fig.show()


# COMMAND ----------

import pandas as pd
import seaborn as sns
from pyspark.sql.functions import hour, count, avg
 
for month in range(1, 13):
    month_df = mdf.toPandas().loc[mdf['relevant_time'].dt.month == month]
    mdf1 = spark.createDataFrame(month_df)
    hourly_trips = mdf1.groupBy(hour("relevant_time").alias("hour")).agg(count("*").alias("num_trips"))
    hourly_trips = hourly_trips.withColumn("hour", hourly_trips["hour"].cast("int"))
    hourly_trips = hourly_trips.groupBy("hour").agg(avg("num_trips").alias("avg_count"))
 
    # Convert the Spark DataFrame to a Pandas DataFrame
    hourly_trips_pd = hourly_trips.toPandas()
 
    # Extract the desired column as a Pandas Series
    hour_of_day = hourly_trips_pd['hour']
 
    # Filter the rows based on the current month
    plot_data = hourly_trips_pd.loc[hour_of_day == month]
 
    # Plot the scatter plot
    sns.scatterplot(x=plot_data['hour'], y=plot_data['avg_count'], label=month)
 
plt.xlabel('Hour of Day')
plt.ylabel('Average Number of Trips')
plt.title('Hourly Trip Trends by Month')
plt.show()


# COMMAND ----------

from pyspark.sql.functions import date_format, count, col, mean
import plotly.graph_objs as go
 
# Define list of common holidays
common_holidays = ["01-01", "02-14", "03-17", "07-04", "08-31", "11-11", "12-25"]
# Create a new column indicating whether the date is a common holiday or not
filtered_mdf = filtered_mdf.withColumn("is_common_holiday", date_format("started_at", "MM-dd").isin(common_holidays))
 
# Group trips by date and whether it's a common holiday or not
daily_trips = filtered_mdf.groupBy("is_common_holiday", date_format("started_at", "MM-dd").alias("date")).agg(count("*").alias("num_trips"))
 
# Calculate the average number of trips per day for each category
avg_daily_trips = daily_trips.groupBy("is_common_holiday").agg(mean(col("num_trips")).alias("avg_daily_trips"))
 
# Convert the Spark DataFrame to a Pandas DataFrame
avg_daily_trips_pd = avg_daily_trips.toPandas()
 
# Create a bar chart
fig = go.Figure(
    go.Bar(
        x=["Non-Holiday", "Common Holiday"],
        y=avg_daily_trips_pd['avg_daily_trips'],
    )
)
 
# Set the axis labels and title
fig.update_layout(
    xaxis_title='Day Type',
    yaxis_title='Average Number of Trips per Day',
    title='Daily Trip Trends: Common Holidays vs. Non-Holidays'
)
 
# Display the plot
fig.show()


# COMMAND ----------



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
