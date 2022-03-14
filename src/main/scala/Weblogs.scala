import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//IN ORDER TO EXECUTE THIS EXERCISE, YOU FIRST NEED TO DOWNLOAD THE FOLLOWING CSV AND STORE IT INSIDE "src/main/resources/"
//LINK:  https://data.world/shad/nasa-website-data


object Weblogs extends App{

  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._



  //load nasa weblogs:
  val weblogsDF = spark.read.option("sep"," ").option("header","true").option("inferSchema","true").csv("src/main/resources/nasa_aug95.csv")
    .where($"request".isNotNull)
  weblogsDF.show()
  weblogsDF.printSchema()

  //List the differents web protocols in the dataset:

  val protocolDF = weblogsDF.withColumn("Protocol",regexp_extract($"request","\\s([A-Z]+/\\w*.\\d*)",1))
    .select("Protocol").distinct()
  protocolDF.show()
  //Group all Status code:

  val statusDF = weblogsDF.groupBy($"status").count().orderBy(desc("count"))
  statusDF.show()

  //Group all the different requests in the dataset:

  val requestDF = weblogsDF.withColumn("requestType",regexp_extract($"request","(\\w*)(\\s)/",1))
    .groupBy($"requestType").count().orderBy(desc("count"))
  requestDF.show()

  //Resource with the max size in the webpage:

  val weblogsResourcesDF = weblogsDF.withColumn("Resource",regexp_extract($"request","\\s(/[\\S ]* )",1))
  weblogsResourcesDF.show()

  val topsize= weblogsResourcesDF.groupBy($"Resource").max("response_size").orderBy(desc("max(response_size)")).limit(1)
  topsize.show()

  //REsource with most traffic:

  val resourceCount = weblogsResourcesDF.groupBy($"Resource").count()

  //Day with most traffic:

  val trafficCount = weblogsDF.withColumn("Date",to_date($"datetime")).groupBy("Date").count()
    .orderBy(desc("count")).limit(1)
  trafficCount.show()

  //Most frequents hosts:

  val hostsDF = weblogsDF.groupBy("requesting_host").count().orderBy(desc("count"))
  hostsDF.show()

  //Hours with most traffic:

  val hoursTraffic = weblogsDF.withColumn("Hour",hour($"datetime")).groupBy("Hour").count().orderBy(desc("count"))
  hoursTraffic.show()

  //Number of 404 errors in each day:

  val errorsCount = weblogsDF.where($"status" === 404).withColumn("Date",to_date($"datetime")).groupBy("Date").count()
    .orderBy(desc("count")).limit(1)
  errorsCount.show()
}
