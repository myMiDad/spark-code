import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MakeRDD {
  def main(args: Array[String]): Unit = {
    //创建rdd的三种方式
    //并行化处理
    val arr: Array[(Int,Int)] = Array((1,1),(2,3),(4,5),(6,7))
    //需要conf
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val rdd1 = sc.parallelize(arr)
//    rdd1.foreach(println(_))

//    val rdd2: RDD[Int] = sc.makeRDD(Seq(1,2,3,4,5,6,7))
//    rdd2.foreach(println(_))

    //外部读入
    val rdd3: RDD[String] = sc.textFile(args(0))
    //rdd转化rdd
    val rdd4: RDD[(String, Int)] = rdd3.map((_,1))

    //释放资源
    sc.stop()
  }
}
