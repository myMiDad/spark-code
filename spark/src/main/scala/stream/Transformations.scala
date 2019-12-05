package stream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transformations {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName(this.getClass.getName)
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //创建一个接收器
    val ssc = new StreamingContext(conf, Seconds(10))

    val topic = "test0722".split(" ")
    //kafka的配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "linux01:9092,linux02:9092,linux03:9092",
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
    //map，有计算逻辑并且返回值的遍历循环
    //    dstream.map((_,1)).print()
    //    dstream.foreachRDD(rdd=>{
    //      rdd.map((_,1)).foreach(println(_))
    //    })

    //flatmap,压平
    //    dstream.map(_.value()).flatMap(_.split(" ")).print()
    //    dstream.foreachRDD(rdd=>{
    ////      rdd.map(_.value()).flatMap(_.split(" ",-1)).foreach(println(_))
    ////    })

    //filter,过滤
    //    dstream.map(_.value()).filter(_.length>=6).print()
    //    dstream.filter(_.value().length != null).print()


    //repatition
    //    dstream.repartition(10)
    //        .foreachRDD(rdd=>{
    //          println(rdd.getNumPartitions)
    //        })

    //reducebykey按照key进行合并
    //    dstream.map(_.value()).map((_,1)).reduceByKey(_+_).print()
    //      dstream.foreachRDD(rdd=>{
    //        rdd.map(_.value()).map((_,1)).reduceByKey(_+_).foreach(println(_))
    //      })


    //groupbykey,按照key进行聚合
    //    dstream.map(_.value()).map((_,1)).groupByKey().print()
    //    dstream.foreachRDD(rdd=>{
    //      rdd.map(_.value()).map((_,1)).groupByKey().foreach(println(_))
    //    })


    //union,两个dstream聚合
    //    val dstream1: DStream[(ConsumerRecord[String, String])] = dstream.map(x => x)
    //    dstream.union(dstream1).print()
    //    dstream.map(_.value()).foreachRDD(rdd=>{
    //      val rdd1 = rdd.map(x=>x)
    //      rdd.union(rdd1).foreach(println(_))
    //    })

    //count()，计数
    //    dstream.count().print()
    //    dstream.foreachRDD(rdd => {
    //      println(rdd.map(_.value()).count())
    //    })

    //reduce,聚合
    //    dstream.reduce((x,y)=>x).print()
    //    dstream.foreachRDD(rdd=>{
    //      rdd.map(_.value()).reduce(_+_).foreach(println(_))
    //    })

    //countbyvalue，给key计数
    //    dstream.map(_.value()).map((_,2)).countByValue().print()
    //    dstream.map(_.value()).foreachRDD(rdd=>{
    //      rdd.map((_,1)).countByValue().foreach(println(_))
    //    })

    //join
    //    val dstream1: DStream[(String, Int)] = dstream.map(_.value()).map((_,2))
    //    dstream.map(_.value()).map((_,1)).join(dstream1).print()
    //    dstream.map(_.value())
    //      .foreachRDD(rdd => {
    //        rdd.map((_, 1)).join(rdd.map((_, 2))).foreach(println(_))
    //      })

    //cogroup
    //    val dstream1 = dstream.map(_.value()).map((_,1))
    //    dstream.map(_.value()).map((_,2)).cogroup(dstream1).print()
//    dstream.map(_.value())
//      .foreachRDD(rdd => {
//        rdd.map((_, 1)).cogroup(rdd.map((_, 2))).foreach(println(_))
//      })

//transform
    dstream.map(_.value()).transform(rdd=>rdd).print()


    //启动程序
    ssc.start()
    ssc.awaitTermination()
  }
}
