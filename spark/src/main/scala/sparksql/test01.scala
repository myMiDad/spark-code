package sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * ClassName: test01
 * Description: 
 *
 * @date 2019/12/3 14:41
 * @author Mi_dad
 */
object test01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.json("file:///E:/test/resources/people.json")
    df.show()
    df.select($"name", $"age" + 1).show()

    spark.stop()
  }

}
