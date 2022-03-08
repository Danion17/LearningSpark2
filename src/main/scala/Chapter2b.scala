import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Chapter2b {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master(master = "local")
      .appName(name = "curso")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val mnmFile = "src/main/resources/mnm_dataset.csv"

    val mnmDF = spark.read.option("header","true").option("inferSchema","true")
      .csv(mnmFile)

    val cntDF = mnmDF.groupBy($"State",$"Color").count()
      .select($"State",$"Color",$"count".as("Total"))
      .orderBy(desc("Total"))

    cntDF.show(60)
    println(s"Total Rows = ${cntDF.count()} \n")
    val calcntDF = mnmDF.where($"State"==="CA")
      .groupBy($"State",$"Color").count()
      .select($"State",$"Color",$"count".as("Total"))
      .orderBy(desc("Total"))
    calcntDF.show(10)


    //Extra exercises:

    //Extra agg operation:
    println("Extra exercises: \n")
    val maxcolorDF = mnmDF.groupBy($"Color").avg("Count").orderBy("avg(Count)")
    maxcolorDF.show()

    //another where clause exercise:
    val nvDF = mnmDF.where($"State" === "NV").groupBy($"State",$"Color").max("Count")
    nvDF.show()

    //df made from various aggregation operations:
    val aggDF = mnmDF.groupBy($"State",$"Color")
      .agg(min("Count").as("min_amount"),avg("Count").as("avg_amount"),max("Count").as("max_amount"))
      .orderBy("State")
    aggDF.show()

    //doing sql queries over a temp view:

    mnmDF.createTempView("sweets")
    spark.sql("SELECT DISTINCT state FROM sweets ORDER BY state").show()
    spark.stop()

  }
}
