import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Actions {
  def main(args: Array[String]): Unit = {
    //创建rdd
    val conf = new SparkConf().setAppName("Transformations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(11, 32, 53, 3, 3, 6, 7, 8))

    //reduce   将数据聚合
    //println(rdd.reduce(_ + _))


    //collect  收集
    //程序运行到从节点上，每个从节点都会产生一个结果，
    // excutor运行程序用的   driver  驱动  监控各个excutor
    //excutor运行的结果在excutor上   driver没有结果
    //如果想让driver得到所有excutor产生的结果  可以利用collect收集到driver
    //结果的位置 从excutor的节点到了driver的节点
    rdd.map(_ * 2).collect()

    //count()   计数
    //println(rdd.count())


    //first()  获取第一个元素
    //    println(rdd.first())


    // take(n)  获取数据集的前n个元素组成的数组
    //println(rdd.take(100).toList)

    //takeOrdered(n)  拿出排序之后的前n个数
    //   println(rdd.takeOrdered(4).toList)

    //saveAsTextFile()  将数据写出到文本中,括号中加的问价路径
    //    rdd.saveAsTextFile("C:\\Users\\thinkpad\\Desktop\\aaaa")

    //saveAsSequenceFile(path) 将数据以SequenceFile形式写出到文件中


    //saveAsObjectFile  将数据以序列化的方式写到文件中
    //    rdd.saveAsObjectFile("C:\\Users\\thinkpad\\Desktop\\aaaa")


    //countByKey  按照key进行计数
    val rdd4: RDD[(String, Int)] = sc.parallelize(List(("kpop", 25), ("apop", 30), ("apop", 15)))
    rdd4.countByKey()


    //foreach()   相当于java中没有返回值的for，遍历循环
//    rdd.foreach(x=>println(x))



    //释放资源
    sc.stop()

  }
}
