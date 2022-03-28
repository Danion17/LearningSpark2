import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinTest extends App{

    val spark = SparkSession.builder()
      .master(master = "local")
      .appName(name = "curso")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val testA = Seq((1,"A","patata"),(1,"B","merluza"),(2,"A","chuche"),(3,"Z","albondigas")).toDF()

    val testB = Seq((1,"A",14.5),(1,"B",17.0),(2,"A",3.03),(3,"Z",3.1416)).toDF()


    testA.join(testB,Seq("_1")).show()

}
