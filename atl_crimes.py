# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, desc, year
from pyspark.sql.types import IntegerType, DoubleType
from dotenv import load_dotenv
import os

load_dotenv()

# https://data.world/bryantahb/crime-in-atlanta-2009-2017

spark = SparkSession.builder.appName("atl-crime").getOrCreate()
df = spark.read.options(header=True,inferSchema=True).csv("atlcrime.csv")

# This dataframe shows the total number of occurences, the average amount of crimes per year, and the average amount of crimes per month during the time period of January 1st 2009 and February 28th (Approximately 8.16 years or 98 months) 

crime_count = df.groupBy(col("crime")).count().withColumn("count",col("count").cast(IntegerType())).orderBy(desc(col("count")))

def avg_per_year(total):
    return round(total / 8.16,2)

def avg_per_month(total):
    return round(total/98, 2)

avgPerYearUDF = udf(lambda x: avg_per_year(x), DoubleType())
avgPerMonthUDF = udf(lambda x: avg_per_month(x), DoubleType())
crime_count = crime_count.withColumn("avg_per_year", avgPerYearUDF(col("count"))).withColumn("avg_per_month", avgPerMonthUDF(col("count")))
print(crime_count.show())

# Dataframe that lists top five most committed crimes during the declared period in descending order

top_five_crime_occurences = crime_count.select("crime")
print(top_five_crime_occurences.show(5))

# 'Larceny-From Vehicle' is described as the top crime occurence from the original data set extracted. This is a dataframe of the top 5 neighborhoods where 'Larceny-From Vehicle' was most common during this period

top_five_vehicle_larceny_neighborhoods = df.filter((df.crime == "LARCENY-FROM VEHICLE") & (col("neighborhood") != "null")).groupBy(df.neighborhood).count().orderBy(desc(col("count"))).select(df.neighborhood)
print(top_five_vehicle_larceny_neighborhoods.show(5))                                                                               

# The year the most crimes occured

year_of_most_crimes = df.groupBy(year("date")).count().orderBy(desc("count")).collect()[0][0]
print(year_of_most_crimes)

# Top Five Neighborhoods with the most rape occurences during this period

top_five_rape_neighborhoods = df.filter((df.crime == "RAPE") & (df.neighborhood != "null")).groupBy(col("neighborhood")).count().orderBy(desc("count"))
print("Top 5 Rape Neighborhoods",top_five_rape_neighborhoods.show(5))

# Top Five Neighborhoods with the most homicide occurences during this period

top_five_homicide_neighborhoods = df.filter((df.crime == "HOMICIDE") & (df.neighborhood != 'null')).select(df.neighborhood).groupBy(df.neighborhood).count().orderBy(desc("count"))
print("Top 5 Murder Neighborhoods", top_five_homicide_neighborhoods.show(5))

# Top Five Neighborhoods with the most aggravated assault occurences during this period

top_five_agg_assault_neighborhoods = df.filter((df.crime == "AGG ASSAULT") & (df.neighborhood != 'null')).select(df.neighborhood).groupBy(df.neighborhood).count().orderBy(desc("count"))
print("Top 5 Aggravated Assault Neighborhoods", top_five_agg_assault_neighborhoods.show(5))


driver = 'org.postgresql.Driver'
table = "atlanta_crimes.total_and_average"
user = os.getenv("USER")
password = os.getenv("PASSWORD")
url = os.getenv("URL") + f'?user={user}&password={password}'
properties = {user: user, password: password, driver: driver}

crime_count.write.jdbc(url, table, properties=properties)
