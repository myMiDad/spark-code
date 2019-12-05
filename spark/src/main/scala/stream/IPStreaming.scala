package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * ClassName: IPStreaming
 * Description:
 *
 * @date 2019/12/5 15:23
 * @author Mi_dad
 */
object IPStreaming {

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

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("E:\\test\\c")
    //kafka的配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //设置一个主题
    val topic = "IPLogs".split(" ")

    val kfStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //位置策略,如果说spark程序和kafka服务不在一个节点上，建议使用PreferConsistent
      //如果spark程序和kafka服务在一个节点上，建议使用PreferBrokers
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topic, kafkaParams
      )
    )

    //读取字典文件
    val ipSource: RDD[String] = sc.textFile("E:\\test\\spark\\查找ip区间\\ip.txt")
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

    val result: DStream[(String, Int)] = kfStream.map(_.value()).map(_.split("\\|", -1)).filter(_.length >= 2)
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
      }).reduceByKeyAndWindow(_ + _,_+_, Seconds(20), Seconds(10))
    result.foreachRDD(rdd=>{
      rdd.foreach(println(_))
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
