package stream

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

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(1))




    ssc.start()
    ssc.awaitTermination()
  }

}
