import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Chapter5 extends App{

  val spark = SparkSession.builder()
    .master(master = "local")
    .appName(name = "curso")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  //APARTADO B)

  //Pros y contras de UDFS:

  //Pros: tienes toda la flexibilidad que el lenguaje te permita (Scala/Java o Python)

  //Contras: Tienes que pensar bien a la hora de crearla (tener en cuenta los nulls y otros posibles valores
  //     que puedan dar problema en tu UDF).
  //          En Python son menos eficientes, hay que utilizar PandasUDF (por la conversión de Python a java)

  // APARTADO C)

  //La diferencia entre dense_rank() y rank() ocurre en caso de empates en el ranking
  //En el caso de rank() el ranking pega saltos Ej: 2 rank 1 empatados, el siguiente seria rank 3
  //En el caso de dense_rank() el ranking nunca pega saltos. Ej: 2 rank 1 empatados, el siguiente sería rank 2.

  //todo intentar solucionar problemas mysql
  


}
