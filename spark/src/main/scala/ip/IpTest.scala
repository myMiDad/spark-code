package ip

import java.util.OptionalInt

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * ClassName: IpTest
 * Description: 
 *
 * @date 2019/12/3 9:10
 * @author Mi_dad
 */
object IpTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //ip的区间地址
    val source1: RDD[String] = sc.textFile(args(0))

    val map = source1.filter(_.length>=14).map(x => {
      val infos = x.split("\\|")
      val min = infos(2).toLong
      val max = infos(3).toLong
      val province = infos(6)
      (min, max,province)
    })
//    println("----------------------------------------------")
//        map.foreach(println(_))
        val broadcast: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(map.collect().sortBy(x=>x._1))

    //访问ip地址
    val source2 = sc.textFile(args(1))
    val map2 = source2.map(x=>ip2Long(x.split("\\|")(1)))
//      .foreach(println(_))
//    map2.foreach(println(_))

    val count = sc.accumulator(0)
    val start = System.currentTimeMillis()
    val result: RDD[(String, Long)] = map2.map(ip => {
      var pro = ""
      var num = 0L
      broadcast.value.foreach(tp => {
        if (ip.toLong >= tp._1 && ip.toLong <= tp._2) {
          pro = tp._3
          num = 1L
        }
        count.add(1)
      })
      (pro, num)
    }).reduceByKey(_+_)
    result.foreach(println(_))
    val end = System.currentTimeMillis()
    println(end-start)
    println(count)



    sc.stop()
  }



  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
