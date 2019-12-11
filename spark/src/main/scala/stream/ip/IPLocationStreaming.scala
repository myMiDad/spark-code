package stream.ip

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import stream.utils.GetRedis


object IPLocationStreaming {

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
      .setMaster("local[1]")
      .setAppName(this.getClass.getName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建一个接收器
    val ssc = new StreamingContext(conf, Seconds(2))

    //读取字典文件
    val ip_dict: RDD[String] = ssc.sparkContext.textFile("E:\\test\\spark\\查找ip区间\\ip.txt")
    //清洗
    val ip_arr: Array[(Long, Long, String)] = ip_dict.map(_.split("\\|", -1)).filter(_.length >= 7).map(arr => (arr(2).toLong, arr(3).toLong, arr(6))).collect()
    //广播变量
    val ip_Bro: Broadcast[Array[(Long, Long, String)]] = ssc.sparkContext.broadcast(ip_arr)

    val topic = "IPLogs".split(" ")
    //kafka的配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test07",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //接收数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topic,
        kafkaParams
      )
    )

    //获取value
    val value_dstream = dstream.map(_.value())
    //切分并过滤
    val filted: DStream[Array[String]] = value_dstream.map(_.split("\\|", -1)).filter(_.length >= 2)
    //拿出ip
    val ip_dstream: DStream[String] = filted.map(arr => arr(1))
    //ip转long类型
    val ip_long: DStream[Long] = ip_dstream.map(ip => ip2Long(ip))
    //获取广播变量
    val ipDict: Array[(Long, Long, String)] = ip_Bro.value
    val provinceAndCount: DStream[(String, Int)] = ip_long.map(ip => {
      //二分查找省份
      val index: Int = binarySearch(ipDict, ip)
      //获取省份
      (ipDict(index)._3, 1)
    }).reduceByKey(_ + _)

    provinceAndCount.print()

    //写到redis中
    provinceAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedis: Jedis = GetRedis.getJedis(7)
        partition.foreach(tp => {
          //插入数据
          jedis.hincrBy("ip",tp._1,tp._2)
        })
        jedis.close()
      })
    })


    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
