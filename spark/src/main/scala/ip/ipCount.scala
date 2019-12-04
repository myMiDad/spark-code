package ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: 王彬
 * @Date: 2019/12/3 9:17
 * @Version 1.0
 */
object ipCount {

  def main(args: Array[String]): Unit = {
    //conf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc: SparkContext = new SparkContext(conf)
    val rddIp: RDD[String] = sc.textFile(args(0))
    val rddLog: RDD[String] = sc.textFile(args(1))

    val ipRdd: RDD[(Long, Long, String)] = rddIp.map(x => {
      val ips: Array[String] = x.split("\\|")
      val ipIndex = ips(2)
      val ipEnd = ips(3)
      val province = ips(6)
      (ipIndex.toLong, ipEnd.toLong, province)
    })
    val bro: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRdd.collect())

    val rddd: RDD[(String, Int)] = rddLog.filter(x=>x.split("\\|").length>=2).map(x => {
      val arr: Array[String] = x.split("\\|")
      var temp: (String, Int) = null
      bro.value.map(x => {
        val ipLong: Long = ip2Long(arr(1))
        if (ipLong >= x._1 && ipLong <= x._2) {
          temp=((x._3), (1))
        }
      })
      temp
    })
    rddd.reduceByKey(_+_).foreach(println(_))

    //释放资源
    sc.stop()
  }

  def binarySearch(arr: Array[(Long, Long, String)], ip: Long): Int = {
    //定义开始的角标和结束的角标
    var min = 0
    var max = arr.length - 1
    //循环
    while (min <= max) {
      //定义一个中间的角标
      val middle = (min + max) / 2
      //判断ip是否在arr（middle）中的开始和结束的ip中间
      if (ip <= arr(middle)._2 && ip >= arr(middle)._1) {
        return middle
      }
      //判断ip是否小于arr(middle)的开始ip
      if (ip < arr(middle)._1) {
        max = middle - 1
      } else {
        min = middle + 1
      }
    }
    return -1
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
