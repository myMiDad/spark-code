package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: KafkaStreaming
 * Description:
 *
 * @date 2019/12/4 21:02
 * @author Mi_dad
 */
object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      //设置优雅的关闭
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      //接kafka的数据，基于网络编程的，序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    //创建一个接收器
    val ssc = new StreamingContext(sc, Seconds(1))
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
    val topic = "test".split(" ")
    //接收kafka的数据
    val kfStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //位置策略,如果说spark程序和kafka服务不在一个节点上，建议使用PreferConsistent
      //如果spark程序和kafka服务在一个节点上，建议使用PreferBrokers
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topic, kafkaParams
      )
    )

    //获取value的值
    val value: DStream[String] = kfStream.map(_.value())
    //切分
    val rdd_flamMap: DStream[String] = value.flatMap(_.split(" "))
    //标记
    val rdd_map: DStream[(String, Int)] = rdd_flamMap.map(line => (line, 1))
    //计数
    val wordCount: DStream[(String, Int)] = rdd_map.reduceByKey(_ + _)
//    val winData = rdd_map.reduceByKeyAndWindow(_+_,Seconds(10))
//    winData.print()

    wordCount.print()

//    kfStream.print()
//    kfStream.foreachRDD(rdd=>{
//      rdd.foreach(println(_))
//    })

//    kfStream.filter(_.value().length>4).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
