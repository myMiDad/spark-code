package friend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * ClassName: ComFriend
 * Description: 
 *
 * @date 2019/12/2 21:02
 * @author Mi_dad
 */
object ComFriend2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //将数据中每行数据 A B,C,D,E,F,G,H,M变为：一个个元组：(B,A),(C,A),...
    val source: RDD[String] = sc.textFile(args(0))
    val mapData1 = source.map(x => {
      val infos = x.split(" ")
      val user = infos(0)
      val friends = infos(1).split(",")
      val tuples = friends.map(friend => (friend, user))
      tuples
    }).saveAsTextFile(args(1))

    sc.stop()
  }

}
