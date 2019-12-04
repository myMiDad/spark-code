import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: RDDTest
 * Description: 
 *
 * @date 2019/11/30 15:30
 * @author Mi_dad
 */
object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val map: RDD[(String, String, String, String, String, Int)] = data.map(x => {
      val splits = x.split(" ")
      (splits(0), splits(1), splits(2), splits(3), splits(4), 1)
    }:(String, String, String, String, String, Int))
    map.foreach(println(_))

    //需求1：统计每一个省份点击TOP3的广告ID
    /**
     * 思路:
     * 1、先将(x,y,z,a,b)按照省份分组映射，在元组最后添加一个点击数量  1
     * 2、按照广告id聚合，点击数量相加
     * 3、根据广告点击数量排序，取top3
     */
    //    map.mapPartitions()
    println("----------------------------------------------------------------------")




    sc.stop()
  }

}
