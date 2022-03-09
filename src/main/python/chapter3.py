# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 17:49:56 2022

@author: daniel.franco
"""

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    
  spark = (SparkSession.builder
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate())
  spark.sparkContext.setLogLevel("ERROR")



  #APARTADO A)

  print("Apartado a):")
  raw_fire_calls_df = (spark.read.option("inferSchema","true").option("header","true")
    .csv("../resources/sf-fire-calls.csv"))

  fire_calls_df = raw_fire_calls_df.withColumn("CallDate",to_date(col("CallDate"),"MM/dd/yyyy"))




  #Exercises page 68 (92 pdf)

  #What were all the different types of fire calls in 2018?
  print("What were all the different types of fire calls in 2018?")
  fire_call_types_2018 = fire_calls_df.filter(year(col("CallDate")) == 2018).select(col("CallType")).distinct()
  fire_call_types_2018.show()



  #What months within the year 2018 saw the highest number of fire calls?
  print("What months within the year 2018 saw the highest number of fire calls?")
  fire_calls_counts_by_month_2018 = (fire_calls_df.filter(year(col("CallDate"))==2018).withColumn("Month",month(col("CallDate")))
    .groupBy("Month").count().select("Month","count"))

  fire_calls_counts_by_month_2018.show()

  #Which neighborhood in San Francisco generated the most fire calls in 2018?
  print("Which neighborhood in San Francisco generated the most fire calls in 2018?")
  top_nh_2018 = (fire_calls_df.filter(year(col("CallDate"))==2018).groupBy(col("Neighborhood")).count()
    .select("Neighborhood").orderBy(desc("count")).limit(1))

  top_nh_2018.show()

  #Which neighborhoods had the worst response times to fire calls in 2018?
  print("Which neighborhoods had the worst response times to fire calls in 2018?")
  worst_response_time_2018 = (fire_calls_df.filter(year(col("CallDate"))==2018).groupBy(col("Neighborhood")).avg("Delay")
    .orderBy(desc("avg(Delay)")).limit(1))

  worst_response_time_2018.show()
  
  # Which week in the year in 2018 had the most fire calls?
  print("Which week in the year in 2018 had the most fire calls?")
  week_most_fire_calls_2018 = (fire_calls_df.filter(year(col("CallDate")) == 2018 ).withColumn("CallWeek",weekofyear(col("CallDate")))
    .groupBy(col("CallWeek")).count().select("CallWeek","count").orderBy(desc("count")).limit(1))
  week_most_fire_calls_2018.show()

  #Is there a correlation between neighborhood, zip code, and number of fire calls?
  print("Is there a correlation between neighborhood, zip code, and number of fire calls?")

  correlation_firecalls = fire_calls_df.groupBy(col("Neighborhood"),col("Zipcode")).count().select("Neighborhood","Zipcode","count")
  correlation_firecalls.show()

  #How can we use Parquet files or SQL tables to store this data and read it back?

  #Using spark.read.format("Parquet")..... for Parquet files or...

  #B)
  print("APARTADO B")

  #Default schema in firecalls dataframe:

  raw_fire_calls_df.printSchema()

  #APARTADO C):

  #El último apartado Booleano significa si puede contener valores nulls o no.

  #APARTADO D):

  #A nivel de código, para crear un dataset se necesita crear una case Class que especifique el formato y tipado de los elementos del dataset

  #Parquet:
  (fire_calls_df.write.format("parquet")
    .mode("overwrite")
    .save("../resources/outputs/firecalls_parquet"))

  #json:

  (fire_calls_df.write.format("json")
    .mode("overwrite")
    .save("../resources/outputs/firecalls_json"))

  #csv:

  (fire_calls_df.write.format("csv")
    .mode("overwrite")
    .save("../resources/outputs/firecalls_csv"))

  #avro:

  (fire_calls_df.write.format("avro")
    .mode("overwrite")
    .save("../resources/outputs/firecalls_avro"))

  #APARTADO F)

  #Que haya más de un fichero asociado al guardar el dataframe se debe al nº de particiones del mismo.
  #Para comprobar el numero de particiones:

  print("APARTADO F)")
  print(fire_calls_df.rdd.getNumPartitions)

  #Para modificar el nº de particiones:

  fire_calls_df2 = fire_calls_df.repartition(1)

  #Ahora volvemos a guarda, solo haremos una sola escritura como ejemplo:

  (fire_calls_df2.write.format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("../resources/outputs/firecalls_csv_1partition"))

    
    
    
if __name__ == "__main__":
    main()