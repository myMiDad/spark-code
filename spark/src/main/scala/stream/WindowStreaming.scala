package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ClassName: WindowStreaming
 * Description: 
 *
 * @date 2019/12/5 13:38
 * @author Mi_dad
 */
object WindowStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")



    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("E:\\test\\d")

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


    val windowStream: DStream[String] = kfStream.map(_.value()).window(Seconds(10),Seconds(3))
    windowStream.print()


//    kfStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
