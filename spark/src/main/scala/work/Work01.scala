package work

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: Work01
 * Description: 
 *
 * @date 2019/12/2 13:50
 * @author Mi_dad
 */
object Work01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val source: RDD[String] = sc.textFile(args(0))

    val intData: RDD[String] = source
      .flatMap(_.split(",")).coalesce(1).sortBy(x=>x.toInt)

    val index: RDD[Int] = sc.makeRDD(0 until intData.count().toInt,1)

    index.zip(intData).foreach(println(_))

//    index.map(x=>(x,intData.))



    sc.stop()
  }

}
