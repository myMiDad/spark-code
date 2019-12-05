package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {

    val updateCount = (values:Seq[Int],state:Option[Int])=>{
      //获取当前的值
      val currentValue = values.foldLeft(0)(_+_)
      //获取历史状态值
      val historyValue: Int = state.getOrElse(0)
      Some(currentValue+historyValue)
    }

    //conf
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建一个接收器
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    //设置检查点
    ssc.checkpoint(".")
    //主题
    val topic = "test".split(" ")
    //kafka的配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop201:9092,hadoop202:9092,hadoop203:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
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
    val value_dstream: DStream[String] = dstream.map(_.value())
    //切分
    val words = value_dstream.flatMap(_.split(" ",-1))
    //wordAndOne
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //调用有状态转换
//    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey[Int](updateCount)
//    result.print()
    //累加的时候，两种方式：
    //1.使用有状态操作updateStateByKey
    //2.使用无状态操作第三方库

    //reducebykeybywindow（窗口大小，步长），每隔10秒计算一下前30s的单词和
//    val wordCounts: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow(_+_,Seconds(30))
//    wordCounts.print()

//    window(窗口大下，步长)
    val dstramWin: DStream[(String, Int)] = wordAndOne.window(Seconds(6),Seconds(4))
    dstramWin.print()

    //countByWindow(窗口大小，步长)
//    dstream.countByWindow(Seconds(4),Seconds(2)).print()


    //reduceByWindow(函数，窗口大小，步长)
//    dstream.map(_.value()).reduceByWindow((x,y)=>x+"|"+y,Seconds(4),Seconds(2)).print()


    //countByValueAndWindow（窗口大小，步长）
//    wordAndOne.countByValueAndWindow(Seconds(4),Seconds(2)).print()

    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
