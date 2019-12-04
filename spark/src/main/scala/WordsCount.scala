import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: WordsCount
 * Description: 
 *
 * @date 2019/11/28 15:17
 * @author Mi_dad
 */
object WordsCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.setMaster("local[*]")
    conf.setAppName("WordsCount")

    val sc = new SparkContext(conf)

        sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile(args(1))
        sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).foreach(println(_))
//        val stringToLong: collection.Map[String, Long] = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).countByKey()
    println("----------|")


    sc.stop()
  }

}
