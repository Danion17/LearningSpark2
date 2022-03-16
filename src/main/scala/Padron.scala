import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Padron extends App{

  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._


  // 6.1 y 6.2
  val padronDF = spark.read.option("sep",";").option("inferSchema","true").option("header","true")
    .option("ignoreTrailingWhiteSpace","true").option("ignoreLeadingWhiteSpace","false")
    .csv("src/main/resources/padron.csv").na.fill(0)
    .withColumn("DESC_DISTRITO",trim($"DESC_DISTRITO"))
    .withColumn("DESC_BARRIO",trim($"DESC_BARRIO"))

  padronDF.show()

  //6.3

  val nhoodEnum = padronDF.select("DESC_BARRIO").distinct()
  nhoodEnum.show()

  //6.4
  padronDF.createTempView("padron")
  spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron").show()

  //6.5

  val padronLongDF = padronDF.withColumn("longitud",length($"DESC_DISTRITO"))
  padronLongDF.show()

  //6.6

  val padron5DF = padronLongDF.withColumn("5",lit(5))
  padron5DF.show()

  //6.7

  val padronDF2 = padron5DF.drop("5")
  padronDF2.show()

  //6.8

  padronDF2.write.mode("overwrite").partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .csv("src/main/resources/outputs/padron")

  //6.9
  padronDF2.persist()

  //6.10

  padronDF2.groupBy($"DESC_DISTRITO",$"DESC_BARRIO")
    .agg(sum($"EspanolesHombres").as("TotalEspanolesHombres"),sum($"EspanolesMujeres").as("TotalEspanolesMujeres"),
      sum($"ExtranjerosHombres").as("TotalExtranjerosHombres"),sum($"ExtranjerosMujeres").as("TotalExtranjerosMujeres"))
    .select("DESC_DISTRITO","DESC_BARRIO","TotalEspanolesHombres","TotalEspanolesMujeres"
    ,"TotalExtranjerosHombres","TotalExtranjerosMujeres").orderBy($"TotalExtranjerosMujeres".desc,$"TotalExtranjerosHombres")
    .show()
  //6.11

  padronDF2.unpersist()

  //6.12
  val totalHombresDF = padronDF2.groupBy($"DESC_DISTRITO",$"DESC_BARRIO").sum("EspanolesHombres")
    .select($"DESC_DISTRITO",$"DESC_BARRIO",$"sum(EspanolesHombres)".as("TotalEspanolesHombres"))

  val joinDF = padronDF2.join(totalHombresDF.as("th"),($"th.DESC_DISTRITO" === padronDF2.col("DESC_DISTRITO"))
  && ($"th.DESC_BARRIO" === padronDF2.col("DESC_BARRIO")))

  joinDF.show(5)

  //6.13

  val windowPadron = Window.partitionBy("DESC_DISTRITO","DESC_BARRIO")

  val padronWindowDF = padronDF2.withColumn("TotalEspanolesHombres",sum($"EspanolesHombres").over(windowPadron))
  padronWindowDF.show()

  //6.14

  val pivotPadron = padronDF2.where($"DESC_DISTRITO" === "BARAJAS" ||$"DESC_DISTRITO" === "CENTRO" || $"DESC_DISTRITO" === "RETIRO")
    .groupBy("COD_EDAD_INT").pivot($"DESC_DISTRITO").sum("EspanolesMujeres").orderBy($"COD_EDAD_INT")

  pivotPadron.show()

  //6.15

  val pivorPercentageDF = pivotPadron.withColumn("BarajasPercentage",$"BARAJAS" / sum($"BARAJAS").over(Window.partitionBy()))
    .withColumn("CentroPercentage",$"CENTRO" / sum($"CENTRO").over(Window.partitionBy()))
    .withColumn("RetiroPercentage",$"RETIRO" / sum($"RETIRO").over(Window.partitionBy()))

  pivorPercentageDF.show()

  //6.16

  padronDF.write.mode("overwrite")
    .partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .csv("src/main/resources/outputs/padronCSV")

  //6.17
  padronDF.write.mode("overwrite")
    .partitionBy("DESC_DISTRITO","DESC_BARRIO")
    .parquet("src/main/resources/outputs/padronParquet")






}
