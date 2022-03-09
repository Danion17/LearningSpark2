import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Chapter4 extends App {

  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  //APARTADO A) EJERCICIOS PROPUESTOS POR EL LIBRO:
  println("APARTADO A")

  val flightsDF = spark.read.option("inferSchema","true").option("header","true")
    .csv("src/main/resources/departuredelays.csv")





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

  //APARTADO B)

  //Una temp view esta asociada a la sparkSession que la creo, desapareciendo cuando esta se deja de ejecutar, sin embargo
  //con Global temp view, no esta atada a una sparkSession, si no a la aplicación spark en general, permaneciendo mientras
  //que siga ejecutandose la aplicación spark aunque se haya dejado de ejecutar la sparkSession que la creo.

  //APARTADO C)

  println("APARTADO C")

  println("PARQUET")

  //Note: in order to execute this part of the code its needed to execute Chapter3 before.

  spark.read.format("parquet").load("src/main/resources/outputs/firecalls_parquet").show(5)

  println("AVRO")
  spark.read.format("avro").load("src/main/resources/outputs/firecalls_avro").show(5)

  println("JSON")
  spark.read.format("json").load("src/main/resources/outputs/firecalls_json").show(5)

  println("csv")
  spark.read.option("inferSchema","true").option("header","true").csv("src/main/resources/outputs/firecalls_csv_1partition")
    .show(5)





}
