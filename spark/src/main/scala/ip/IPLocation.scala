package ip

import java.sql.DriverManager

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IPLocation {

  //ip字符串转long类型
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
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

  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    //sc
    val sc = new SparkContext(conf)
    //读取字典文件
    val ipSource = sc.textFile("C:\\Users\\thinkpad\\Desktop\\查找ip区间\\ip.txt")
    //处理字典文件
    val ipdictRDD: RDD[(Long, Long, String)] = ipSource.map(_.split("\\|")).filter(_.length >= 7)
      .map(arr => {
        val ipStart = arr(2).toLong
        val ipEnd = arr(3).toLong
        val provincename = arr(6)
        (ipStart, ipEnd, provincename)
      })
    //手机数据
    val dictArr: Array[(Long, Long, String)] = ipdictRDD.collect().sortBy(x => x._1)
    //广播变量
    val dictBro: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(dictArr)

    //读取原始日志文件
    val source = sc.textFile("C:\\Users\\thinkpad\\Desktop\\查找ip区间\\access.log")
    //数据清洗
    val result: RDD[(String, Int)] = source.map(_.split("\\|", -1)).filter(_.length >= 2)
      .map(arr => {
        //获取ip
        val ip = arr(1)
        //ip转long
        val ipNum: Long = ip2Long(ip)
        //获取广播变量的值
        val dictArr: Array[(Long, Long, String)] = dictBro.value
        //调用二分查找的方法
        val index: Int = binarySearch(dictArr: Array[(Long, Long, String)], ipNum: Long)
        //找到index所对应的数组元素
        var province = ""
        if (index != -1) {
          province = dictArr(index)._3
        }
        (province, 1)
      }).reduceByKey(_ + _)
    //写入到mysql中
    result.foreach(tp=>{
      //获取mysql的连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?characterEncoding=utf-8","root","root")
      //创建sql语句
      val stat = conn.prepareStatement("insert into ip0722 values (?,?)")
      //绑定参数
      stat.setString(1,tp._1)
      stat.setLong(2,tp._2)
      //执行sql
      stat.executeUpdate()
      conn.close()
    })



    //释放资源
    sc.stop()
  }
}
