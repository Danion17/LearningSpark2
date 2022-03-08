# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 17:49:56 2022

@author: daniel.franco
"""

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    
    spark = (SparkSession.builder.master(master = "local")
      .appName(name = "curso")
      .config("spark.some.config.option", "some-value")
      .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    
    
    print("apartado A")
    flights_df = (spark.read.option("inferSchema","true").option("header","true")
   .csv("../resources/departuredelays.csv").repartition(4))
    
    flights_df.createTempView("us_delay_flights_tbl")
    
    spark.sql("SELECT * FROM us_delay_flights_tbl").show()
    
    
    flights_mod_df = flights_df.select(substring(col("date"),2,2).alias("day"),substring(col("date"),0,2).alias("month")
                                       ,col("delay"),col("distance"),col("origin"),col("destination"))
    
    flights_mod_df.show()
    
    #Adapt to dataframes api the followings slq queries:
        
    #Query1
    spark.sql("""SELECT date, delay, origin, destination
              FROM us_delay_flights_tbl
              WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
              ORDER by delay DESC""").show(10)
    
    query1 = (flights_df.filter(col("delay") >  120 and col("origin") == "SFO" and col("destination") == "ORD")
                 .orderBy(desc("delay")).select("date","delay","origin","destination"))
    
    #Query2:
        
    spark.sql("""SELECT delay, origin, destination,
                 CASE
                 WHEN delay > 360 THEN 'Very Long Delays'
                 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                 WHEN delay = 0 THEN 'No Delays'
                 ELSE 'Early'
                 END AS Flight_Delays
                 FROM us_delay_flights_tbl
                 ORDER BY origin, delay DESC""").show(10)
                 
    query2 = (flights_df.withColumn("Flight_Delays", when(col("delay") > 360,"Very Long Delays")
                                   .when(col("delay") > 120,"Long Delays")
                                   .when(col("delay") > 60,"Short Delays")
                                   .when(col("delay") > 0,"Torelable Delays")
                                   .otherwise("No delays")).select("delay","origin","destination","Flight_Delays")
              .orderBy(col("origin"),col("delay").desc))
    
    
    query2.show()
    
    
    #For the written answers check the scala version of this chapter
    print("apartadoB")
    flights_df.printSchema()
    
    
    #Apartado E
    
    #Parquet:
        
    flights_df.write.format("parquet").mode("overwrite").save("../resources/outputs/flight_parquet")
     
    #json:
        
    flights_df.write.format("json").mode("overwrite").save("../resources/outputs/flight_parquet")
    
    #csv:
        
    flights_df.write.format("csv").mode("overwrite").save("../resources/outputs/flight_parquet")
    
    #avro:
        
    flights_df.write.format("avro").mode("overwrite").save("../resources/outputs/flight_parquet")
    
    
    print("apartado f")
    
    print(flights_df.rdd.getNumPartitions)
    
    flights_df2 = flights_df.repartition(1)
    
    flights_df2.write.format("csv").mode("overwrite").save("../resources/outputs/flight_parquet")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    main()