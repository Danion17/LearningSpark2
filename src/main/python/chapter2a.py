

from pyspark.sql import SparkSession


def main():
    
    
    spark =( SparkSession.builder.appName("curso").master("local")
    .config("spark.some.config.option", "some-value").getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    
    
    dataset_filepath= "../resources/el_quijote.txt"
    quijote_txt = spark.read.text(dataset_filepath)
    
    print(quijote_txt)
    
    
    
    
    
if __name__ == "__main__":
    main()
    