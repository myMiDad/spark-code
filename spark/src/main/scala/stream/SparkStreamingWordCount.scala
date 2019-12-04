package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    //设置优雅的关闭
      .set("spark.streaming.stopGracefullyOnShutdown","true")
    //接kafka的数据，基于网络编程的，序列化
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    //创建一个接收器
    val ssc: StreamingContext = new StreamingContext(conf,Milliseconds(500))

    //设置一个主题
    val topic = "test0722".split(" ")

    //kafka的配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "linux01:9092,linux02:9092,linux03:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //接收kafka的数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //位置策略,如果说spark程序和kafka服务不在一个节点上，建议使用PreferConsistent
      //如果spark程序和kafka服务在一个节点上，建议使用PreferBrokers
      LocationStrategies.PreferConsistent,
      //消费策略
      ConsumerStrategies.Subscribe[String, String](
        topic,
        kafkaParams
      )
    )
    //ConsumerRecord(topic = test0722,
    // partition = 1,
    // offset = 0,
    // CreateTime =
    // 1575039299752,
    // checksum = 1754176964,
    // serialized key size = -1
    // , serialized value size = 6,
    // key = null,
    // value = hadoop)
    //获取value值
    val rdd_value: DStream[String] = dstream.map(_.value())
    //切分
    val filted:DStream[String] = rdd_value.flatMap(_.split(" ",-1))
    //标记1
    val wordAndOne: DStream[(String, Int)] = filted.map((_,1))
    //计数
    val wordAndCount = wordAndOne.reduceByKey(_+_)
    wordAndCount.print()


    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
