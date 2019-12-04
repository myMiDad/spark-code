package friend

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: WebTop
 * Description: 
 *
 * @date 2019/12/3 8:34
 * @author Mi_dad
 */
object WebTop {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      val sc = new SparkContext(conf)

    val source = sc.textFile(args(0))
    val splits: RDD[String] = source.flatMap(_.split("//"))



    sc.stop()
  }

}
