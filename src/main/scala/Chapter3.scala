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
}
