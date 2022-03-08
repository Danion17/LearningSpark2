

from pyspark.sql import SparkSession


def main():
    
    
    spark =( SparkSession.builder.appName("curso").master("local")
    .config("spark.some.config.option", "some-value").getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    
    
    dataset_filepath= "../resources/el_quijote.txt"
    quijote_txt = spark.read.text(dataset_filepath)
    
    print(quijote_txt.count())
    
    print("---------------------------------------------")
    quijote_txt.show()
    quijote_txt.show(truncate=False)
    quijote_txt.show(10)
    quijote_txt.show(10,False)
    print("---------------------------------------------")
    print(quijote_txt.head(1))   #head(x) == take(x), pero head tiene la opción de ir sin parametro, que te devuelve directamente los datos en vez de un objeto row
    print(quijote_txt.first())   #first() == head()
    print(quijote_txt.take(1))
    print("---------------------------------------------")
    
    
    
    
    
if __name__ == "__main__":
    main()
    