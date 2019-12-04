import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: RDDCache
 * Description: 
 *
 * @date 2019/11/30 17:23
 * @author Mi_dad
 */
object RDDCache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc: SparkContext = new SparkContext(conf)

    val list: RDD[Int] = sc.makeRDD(1 to 10)
    /*val noCacheList: RDD[String] = list.map(_.toString+"--"+System.currentTimeMillis()+"--")
    val cacheList: RDD[String] = list.map(_.toString+"--"+System.currentTimeMillis()+"--").cache()

    println(noCacheList.collect().toBuffer)
    println(noCacheList.collect().toBuffer)
    println(noCacheList.collect().toBuffer)
    println(noCacheList.collect().toBuffer)
    println(noCacheList.collect().toBuffer)
    println("---------------------------------------------")
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)
    println(cacheList.collect().toBuffer)*/
    sc.setCheckpointDir(args(0))
    val checkPointList: RDD[String] = list.map(_.toString+"=="+System.currentTimeMillis()+"==")
    checkPointList.checkpoint()
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)
    println(checkPointList.collect().toBuffer)



  }

}
