package sparksql.work

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * ClassName: SqlSort
 * Description: 
 *
 * @date 2019/12/4 14:00
 * @author Mi_dad
 */
object SqlSort {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._

    val sourceDS: Dataset[String] = session.read.textFile("E:\\test\\spark\\work\\成绩前N+二次排序.txt")
    val dataDS: Dataset[(String, Int, Int)] = sourceDS.map(line => {
      val splits = line.split(",")
      (splits(0).trim, splits(1).trim.toInt, splits(2).trim.toInt)
    })
    val dataDF: DataFrame = dataDS.toDF("name","class","score")
    dataDF.createTempView("data")
    session.sql(
      """
        |select * from data class order by score desc
        |""".stripMargin).show()

    session.stop()
  }

}
