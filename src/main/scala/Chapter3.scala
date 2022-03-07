import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Chapter3 extends App {
  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  //APARTADO A) EJERCICIOS PROPUESTOS POR EL LIBRO:
  println("APARTADO A")


  //Nota: por defecto 1 partición, necesitamos más para realizar el apartado f)
  val flightsDF = spark.read.option("inferSchema","true").option("header","true")
    .csv("src/main/resources/departuredelays.csv").repartition(4)





  flightsDF.createTempView("us_delay_flights_tbl")

  spark.sql("SELECT * FROM us_delay_flights_tbl").show()


  //Ejercicio pag 87 libro 111 pdf:  usar un formato de fecha adecuado:
  //Pasaremos de una columna date a 2 columnas day y month:

  val flightsModDF = flightsDF.select(substring($"date",2,2).as("day"),substring($"date",0,2).as("month")
  ,$"delay",$"distance",$"origin",$"destination")

  flightsModDF.show()

  //Ejercicio: usa las siguientes queries usando la api de dataframes:

  //Query 1:
  spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

  val query1 = flightsDF.filter($"delay">120 && $"origin" === "SFO" && $"destination" === "ORD")
    .orderBy(desc("delay")).select("date","delay","origin","destination")
  query1.show()

  //Query2:
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

  val query2 = flightsDF.orderBy($"origin",$"delay".desc).withColumn("Flight_Delays",
    when($"delay"> 360,"Very Long Delays")
    .when($"delay" > 120,"Long Delays")
    .when($"delay" > 60,"Short Delays")
    .when($"delay" > 0,"Tolerable Delays")
    .otherwise("No delays")).select("delay","origin","destination","Flight_Delays")
  query2.show()

  //APARTADO B):
  println("APARTADO B")

  //Esquema hayado por defecto en datasets de vuelos:

  flightsDF.printSchema()

  //APARTADO C):

  //El último apartado Booleano significa si puede contener valores nulls o no.

  //APARTADO D):

  //A nivel de código, para crear un dataset se necesita crear una case Class que especifique el formato del dataset

  //APARTADO E):

  //NOTA: aumentamos a mano el nº de partitions porque por defecto es 1 en este caso, y queremos que sea >1 para
  //los ejercicios
  //flightsDF.repartition(2)
  //Parquet:
  flightsDF.write.format("parquet")
    .mode("overwrite")
    .save("src/main/resources/outputs/flight_parquet")

  //json:

  flightsDF.write.format("json")
    .mode("overwrite")
    .save("src/main/resources/outputs/flight_json")

  //csv:

  flightsDF.write.format("csv")
    .mode("overwrite")
    .save("src/main/resources/outputs/flight_csv")

  //avro:

  flightsDF.write.format("avro")
    .mode("overwrite")
    .save("src/main/resources/outputs/flight_avro")


  //APARTADO F)

  //Que haya más de un fichero asociado al guardar el dataframe se debe al nº de particiones del mismo.
  //Para comprobar el numero de particiones:

  println("APARTADO F)")
  println(flightsDF.rdd.getNumPartitions)

  //Para modificar el nº de particiones:

  val flightsDF2 = flightsDF.repartition(1)

  //Ahora volvemos a guarda, solo haremos una sola escritura como ejemplo:

  flightsDF2.write.format("csv")
    .mode("overwrite")
    .save("src/main/resources/outputs/flight_csv_1partition")










}
