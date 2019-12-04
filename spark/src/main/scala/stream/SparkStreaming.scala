package stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ClassName: SparkStraming
 * Description:
 *
 * @date 2019/12/4 15:00
 * @author Mi_dad
 */
object SparkStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)


    val sc = new StreamingContext(conf,Seconds(1))

    val line: ReceiverInputDStream[String] = sc.socketTextStream("hadoop201",9999)
    line.flatMap(_.split(" ")).map(line=>(line,1)).reduceByKey(_+_).print()

    sc.start()
    sc.awaitTermination()
  }
}
