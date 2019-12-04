import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: ReduceByKeyTest
 * Description: 
 *
 * @date 2019/11/30 22:47
 * @author Mi_dad
 */
object ReduceByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)
    val list: RDD[String] = sc.makeRDD(List("a","b","c","a","b","c","a","b","c","a","b","c","a","b","c","a","b","c"))
    println(list.getNumPartitions)
    list.map((_,1)).reduceByKey((x,y)=>x+y).foreach(println(_))
//    list.map((_,1)).groupByKey().reduce((x,y)=>


  }
}
