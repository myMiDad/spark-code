package sparksql.work

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * ClassName: SparkSqlTest01
 * Description: 
 *
 * @date 2019/12/4 13:28
 * @author Mi_dad
 */
object SparkSqlTest01 {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    val ds: Dataset[String] = session.read.textFile("E:\\test\\spark\\work\\排序加序号.txt")

    val data: RDD[Int] = ds.flatMap(_.split(",")).map(_.trim.toInt).coalesce(1).rdd.sortBy(x=>x)

    val index: RDD[Int] = session.sparkContext.makeRDD(0 until data.count().toInt,1)

    val dataDF: DataFrame = index.zip(data).toDF("Index","Number")


    dataDF.createTempView("num")
    session.sql(
      """
        |select * from num order by Number
        |""".stripMargin).show()

index.zip(data).foreach(println(_))

    session.stop()
  }

}
