import org.apache.spark.sql.SparkSession

object Chapter2a extends App {

  implicit val sparkSession = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val quijote = sparkSession.read.text("src/main/resources/el_quijote.txt")

  println(quijote.count())

  println("---------------------------------------------")
  quijote.show()
  quijote.show(truncate=false)
  quijote.show(10)
  quijote.show(10,false)
  println("---------------------------------------------")
  println(quijote.head(1))   //head(x) == take(x), pero head tiene la opción de ir sin parametro, que te devuelve directamente los datos en vez de un objeto row
  println(quijote.first())  //first() == head()
  println(quijote.take(1))
  println("---------------------------------------------")



}
