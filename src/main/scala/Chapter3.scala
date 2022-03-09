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


  //APARTADO A)

  println("Apartado a):")
  val rawFireCallsDF = spark.read.option("inferSchema","true").option("header","true")
    .csv("src/main/resources/sf-fire-calls.csv")

  val fireCallsDF = rawFireCallsDF.withColumn("CallDate",to_date($"CallDate","MM/dd/yyyy"))




  //Exercises page 68 (92 pdf)

  // What were all the different types of fire calls in 2018?
  println("What were all the different types of fire calls in 2018?")
  val fireCallsTypes2018 = fireCallsDF.filter(year($"CallDate") === 2018).select($"CallType").distinct()
  fireCallsTypes2018.show()




  // What months within the year 2018 saw the highest number of fire calls?
  println("What months within the year 2018 saw the highest number of fire calls?")
  val fireCallsCountsByMonth2018 = fireCallsDF.filter(year($"CallDate")===2018).withColumn("Month",month($"CallDate"))
    .groupBy("Month").count().select("Month","count")

  fireCallsCountsByMonth2018.show()

  //Which neighborhood in San Francisco generated the most fire calls in 2018?
  println("Which neighborhood in San Francisco generated the most fire calls in 2018?")
  val topNbhoodFC2018= fireCallsDF.filter(year($"CallDate")===2018).groupBy($"Neighborhood").count()
    .select("Neighborhood").orderBy(desc("count")).limit(1)

  topNbhoodFC2018.show()

  // Which neighborhoods had the worst response times to fire calls in 2018?
  println("Which neighborhoods had the worst response times to fire calls in 2018?")
  val worstResponseTime2018 = fireCallsDF.filter(year($"CallDate")===2018).groupBy($"Neighborhood").avg("Delay")
    .orderBy(desc("avg(Delay)")).limit(1)

  worstResponseTime2018.show()
  // Which week in the year in 2018 had the most fire calls?
  println("Which week in the year in 2018 had the most fire calls?")
  val weekMostFireCalls2018 = fireCallsDF.filter(year($"CallDate") === 2018 ).withColumn("CallWeek",weekofyear($"CallDate"))
    .groupBy($"CallWeek").count().select("CallWeek","count").orderBy(desc("count")).limit(1)
  weekMostFireCalls2018.show()

  //Is there a correlation between neighborhood, zip code, and number of fire calls?
  println("Is there a correlation between neighborhood, zip code, and number of fire calls?")

  val correlationFireCallsDF = fireCallsDF.groupBy($"Neighborhood",$"Zipcode").count().select("Neighborhood","Zipcode","count")
  correlationFireCallsDF.show()

  //How can we use Parquet files or SQL tables to store this data and read it back?

  //Using spark.read.format("Parquet")..... for Parquet files or...

  //B)
  println("APARTADO B")

  //Default schema in firecalls dataframe:

  rawFireCallsDF.printSchema()

  //APARTADO C):

  //El último apartado Booleano significa si puede contener valores nulls o no.

  //APARTADO D):

  //A nivel de código, para crear un dataset se necesita crear una case Class que especifique el formato y tipado de los elementos del dataset

  //Parquet:
  fireCallsDF.write.format("parquet")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_parquet")

  //json:

  fireCallsDF.write.format("json")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_json")

  //csv:

  fireCallsDF.write.format("csv")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_csv")

  //avro:

  fireCallsDF.write.format("avro")
    .mode("overwrite")
    .save("src/main/resources/outputs/firecalls_avro")

  //APARTADO F)

  //Que haya más de un fichero asociado al guardar el dataframe se debe al nº de particiones del mismo.
  //Para comprobar el numero de particiones:

  println("APARTADO F)")
  println(fireCallsDF.rdd.getNumPartitions)

  //Para modificar el nº de particiones:

  val fireCallsDF2 = fireCallsDF.repartition(1)

  //Ahora volvemos a guarda, solo haremos una sola escritura como ejemplo:

  fireCallsDF2.write.format("csv")
    .mode("overwrite")
    .option("header","true")
    .save("src/main/resources/outputs/firecalls_csv_1partition")


  //todo mover todo lo de abajo

  /*
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

  val query2 = flightsDF.withColumn("Flight_Delays",
    when($"delay"> 360,"Very Long Delays")
    .when($"delay" > 120,"Long Delays")
    .when($"delay" > 60,"Short Delays")
    .when($"delay" > 0,"Tolerable Delays")
    .otherwise("No delays")).select("delay","origin","destination","Flight_Delays")
    .orderBy($"origin",$"delay".desc)
  query2.show()

  //APARTADO B):
  println("APARTADO B")

  //Esquema hayado por defecto en datasets de vuelos:

  flightsDF.printSchema()

  //APARTADO C):

  //El último apartado Booleano significa si puede contener valores nulls o no.

  //APARTADO D):

  //A nivel de código, para crear un dataset se necesita crear una case Class que especifique el formato y tipado de los elementos del dataset

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









  */
}
