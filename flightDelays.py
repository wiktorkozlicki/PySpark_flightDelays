# This python file contains py-spark code. Once executed in terminal, it returns results, copied by me to the results.txt file.

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, desc, count, rank
from pyspark.sql.window import Window


# Creating Spark Session:
spark = (SparkSession
         .builder
         .appName("flight_delays")
         .getOrCreate())


# Path to CSV file we'll be using:
csv_file = "/home/wiktor/Documents/sparb2/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# Create schema for our DataFrame:
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

# Creating DataFrame from our CSV file, using schema defined above:
df=spark.read.schema(schema).option("header","true").csv(csv_file)

###1### Let's look at our records by selecting all columns:
df.select("date","delay","distance","origin","destination").orderBy("date").show(10)

###2### Use "where()" to select all flights with origin in New York:
# I don't know what is the code for New York City. I think they should have letter "N" in their name:
df.select("origin").where(col("origin").like("%N%")).distinct().show(100)

# As it turns out there's no origin like "NY" or "NYC", we have to search for particular airports.
###3### Let's select 3 major ones in New York:
df.select("date","delay","distance","origin","destination")\
    .where((col("origin") == "LGA") | (col("origin") == "JFK") | (col("origin") == "EWR"))\
    .orderBy("date").show(10)

###4### Let's select airport with most flights:
df.groupBy("origin").count().orderBy("count", ascending=False).withColumnRenamed("count","number_of_flights").show(10)

###5### Let's see average delay for each airport:
df.groupBy("origin").agg(avg(col("delay"))).withColumnRenamed("avg(delay)","average_delay")\
    .orderBy("average_delay",ascending=False).show(10)

###6### Final query: Ranking of average delays, with origin in LGA, JFK and EWR airports, partitioned by destination airport.
df_ny = df.select("origin","destination","delay")\
    .where((col("origin") == "LGA") | (col("origin") == "JFK") | (col("origin") == "EWR"))\
        .groupBy("origin","destination").agg(avg(col("delay"))).withColumnRenamed("avg(delay)","average_delay")

windowSpec= Window.partitionBy("origin").orderBy(desc("average_delay"))

df_ny.withColumn("rank",rank().over(windowSpec)).orderBy("rank",desc("average_delay")).show()
