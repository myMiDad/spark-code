package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.immutable.HashMap

/**
 * ClassName: SqlTest
 * Description: 
 *
 * @ date 2019/12/3 19:29
 * @ author Mi_dad
 */
object SqlTest {
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._
    val ds: Dataset[String] = session.read.textFile("file:///E:/test/resources/people.txt")
//    ds.show()

    ds.createTempView("people")
    //    session.sql("select * from people").show()

    //读取的JSON，使用df操作的
//    val dataDf: DataFrame = session.sql(
//      """
//        |select
//        |   *
//        |from
//        |   people
//        |where
//        |   age>19
//      """
//        .stripMargin)

    val value: RDD[(String, String)] = ds.rdd.map(x=>(x.split(",")(0).trim,x.split(",")(1).trim))
//    value.foreach(println(_))

    val dataDF: DataFrame = value.toDF("name","age")
    dataDF.createOrReplaceTempView("people")

//    session.sql(
//      """select
//        | *
//        |from
//        | people
//        |""".stripMargin).show()

    //方式一
//    val dataDS: Dataset[(String, String)] = value.toDS()

//    dataDS.createOrReplaceTempView("people")
//    dataDS.show()
    //方式二
    val rddPeople: RDD[People] = value.map(line =>People(line._1, line._2.toInt))
    val pds: Dataset[People] = rddPeople.toDS()

    val frame: DataFrame = value.map(line=>People(line._1,line._2.toInt)).toDF()
    val value1: Dataset[People] = frame.as[People]

        val rdd1: RDD[People] = pds.rdd
    println(rdd1.map(_.name).collect().toBuffer)



    session.stop()

  }
  //定义字段名和类型
  case class People(name:String,age:Int) extends Serializable

}
