# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 13:46:12 2022

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

    
    mnm_filepath = "../resources/mnm_dataset.csv"
    
    mnm_df = spark.read.option("inferSchema","true").option("header","true").csv(mnm_filepath)
    
    count_mnm = (mnm_df.groupBy(col("State"),col("Color")).count()
                     .select(col("State"),col("Color"),col("count").alias("Total"))
                     .orderBy(desc("Total")))
    
    count_mnm.show()
    print("Total Rows: ",count_mnm.count())
    
    
    ca_count_mnm = (mnm_df.filter(col("State") == "CA")
                    .groupBy(col("State"),col("Color")).count()
                     .select(col("State"),col("Color"),col("count").alias("Total"))
                     .orderBy(desc("Total")))
    
    ca_count_mnm.show(10)
    
    
    #Extra exercises:
    
    #Extra agg operation:
    max_color_mnm = mnm_df.groupBy(col("Color")).avg("Count").orderBy("avg(Count)")
    max_color_mnm.show()
    
    #Extra where/filter clause exercise:
    nv_max_mnm = mnm_df.filter(col("State") == "NV").groupBy(col("State"),col("Color")).max("Count")
    nv_max_mnm.show()
    
    #dataframe made from various agg operations:
        
    agg_mnm = (mnm_df.groupBy(col("State"),col("Color"))
      .agg(min("Count").alias("min_amount"),avg("Count").alias("avg_amount"),max("Count").alias("max_amount"))
      .orderBy("State"))
    agg_mnm.show()
    
    #doing sql queries over a temp view:
        
    mnm_df.createTempView("sweets")
    
    spark.sql("SELECT DISTINCT state FROM sweets ORDER BY state").show()
    spark.stop()
    


    
if __name__ == "__main__":
    main()