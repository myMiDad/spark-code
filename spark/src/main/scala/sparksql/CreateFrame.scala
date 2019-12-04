package sparksql

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * ClassName: CreateFrame
 * Description: 
 *
 * @date 2019/12/3 16:53
 * @author Mi_dad
 */
object CreateFrame {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
//      .config("spark.some.config.option","some-value")
      .getOrCreate()

    import session.implicits._

    val df: DataFrame = session.read.json("file:///E:/test/resources/people.json")


//    df.printSchema()

//    df.select("*").show()

//    df.select("name","age").show()

//    df.select($"name",$"age"+1).show()

    df.filter($"age">19).show()

    df.groupBy("name").sum("age").show()
    df.groupBy("name").count().show()

//    df.groupBy("age").count().show()




    session.stop()


  }

}
