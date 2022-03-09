# -*- coding: utf-8 -*-
"""
Created on Wed Mar  9 12:08:32 2022

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
    
    
    #APARTADO A) EJERCICIOS PROPUESTOS POR EL LIBRO:
    
    print("APARTADO A")
    
    flights_df = (spark.read.option("inferSchema","true").option("header","true")
    .csv("../resources/departuredelays.csv"))
    
    flights_df.createOrReplaceTempView("us_delay_flights_tbl")
    
      
    spark.sql("SELECT * FROM us_delay_flights_tbl").show()
    
    
    #Ejercicio pag 87 libro 111 pdf:  usar un formato de fecha adecuado:
    #Pasaremos de una columna date a 2 columnas day y month:
    
    flights_mod_df = (flights_df.select(substring(col("date"),2,2).alias("day"),substring(col("date"),0,2).alias("month")
    ,col("delay"),col("distance"),col("origin"),col("destination")))
    
    flights_mod_df.show()
    
    #Ejercicio: usa las siguientes queries usando la api de dataframes:
    
    #Query 1:
      
    spark.sql("""SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)
    
    query1 = (flights_df.filter((col("delay")>120 ) & ( col("origin") == "SFO" ) & ((col("destination") == "ORD")))
    .orderBy(desc("delay")).select("date","delay","origin","destination"))
    query1.show()
    
   
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
    
    query2 = (flights_df.withColumn("Flight_Delays",
    when(col("delay")> 360,"Very Long Delays")
      .when(col("delay") > 120,"Long Delays")
      .when(col("delay") > 60,"Short Delays")
      .when(col("delay") > 0,"Tolerable Delays")
      .otherwise("No delays")).select("delay","origin","destination","Flight_Delays")
    .orderBy("origin",desc("delay")))
    query2.show()
    #APARTADO B)
    
     #Una temp view esta asociada a la sparkSession que la creo, desapareciendo cuando esta se deja de ejecutar, sin embargo
     #con Global temp view, no esta atada a una sparkSession, si no a la aplicación spark en general, permaneciendo mientras
     #que siga ejecutandose la aplicación spark aunque se haya dejado de ejecutar la sparkSession que la creo.
    
    #APARTADO C)
    
    print("APARTADO C")
    
    print("PARQUET")
    
    #Note: in order to execute this part of the code its needed to execute Chapter3 before.
    
    spark.read.format("parquet").load("../resources/outputs/firecalls_parquet").show(5)
    
    
    
    #####TODO problema con leer avro en pypspark
    print("AVRO")
    spark.read.format("avro").load("../resources/outputs/firecalls_avro").show(5)
    
    print("JSON")
    spark.read.format("json").load("../resources/outputs/firecalls_json").show(5)
    
    print("csv")
    spark.read.option("inferSchema","true").option("header","true").csv("../resources/outputs/firecalls_csv_1partition").show(5)
    
    spark.stop()
    
    
if __name__ == "__main__":
    main()