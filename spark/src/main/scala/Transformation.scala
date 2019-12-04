import java.sql.{DriverManager, PreparedStatement, ResultSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * ClassName: Transformation
 * Description: 
 *
 * @date 2019/11/29 13:49
 * @author Mi_dad
 */
object Transformation {
  def main(args: Array[String]): Unit = {
    //获取conf
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Transformation")
    //获取SparkContext
    val sc: SparkContext = new SparkContext(conf)

    //设置数据源
    val source: RDD[String] = sc.textFile(args(0))

    println(source.getNumPartitions)

//    println("---------------------source-----------------------------")
    //原数据遍历
    //    source.foreach(println)


//    println("----------------------map-------------------------------")
    //映射为map
//    val map: RDD[(String, Int)] = source.map((_, 1))
    //    map.foreach(println(_))

//    println("----------------------flatMap---------------------------")
    //对数据进行切分处理
    val flatMap: RDD[String] = source.flatMap(_.split(" "))
    //    flatMap.foreach(println(_))
    //    flatMap.map((_,1)).foreach(println(_))

//    println("----------------------mapPartitions---------------------------")
//    val value = source.mapPartitions(partition => {
//      partition.flatMap(_.split(" ")).map((_, 1))
//    })
//        println("----------------------mapPartitions---------------------------")
//    flatMap.mapPartitions(x=>Iterator(x.mkString("|"))).foreach(println(_))
    //    value.foreach(println(_))
    //    value.reduceByKey((x,y)=>x+y).foreach(println(_))
List(11,11).mkString(" ")
//    println("----------------------filter---------------------------")
//    val filter: RDD[(String, Int)] = value.reduceByKey((x, y) => x + y).filter(_._2 > 1)
//    filter.foreach(println(_))

//    println("----------------------mapPartitionWithIndex---------------------------")
//    def partitionFun(index:Int,iterator: Iterator[(String,Int)]):Iterator[(String,Int)]={
//      println("-------------")
//      while (iterator.hasNext){
//        val tp = iterator.next()
//        println(tp._1)
//      }
//      iterator
//    }
//    source.flatMap(_.split(" ")).map((_, 1)).mapPartitionsWithIndex(partitionFun)
//

    //distinct(numtasks)去重操作 numtasks申请多少个任务来执行
    //默认启动一个task，但是distinct在工作中不建议使用，会造成shuffle
//    flatMap.map((_,1)).distinct(8).foreach(println(_))

    //coalesce  修改分区数 分区数变少的时候建议用coalesce
//    println(flatMap.partitions.size)
//    val partitionMap = flatMap.map((_,1)).coalesce(1)
//    println(partitionMap.partitions.size)

  //repartition  修改分区数 分区数变多的时候建议使用
//    val repartitionPartition = flatMap.map((_,1)).repartition(5)
//    println(repartitionPartition.partitions.size)

//    flatMap.map((_,1)).repartitionAndSortWithinPartitions(Partitioner.defaultPartitioner(flatMap.map((_,1)))).foreach(println(_))

//    flatMap.map((_,1)).reduceByKey(_+_).sortBy(word=>word._2,false,1).foreach(println(_))

    println("------------------------------------------------------------")
    val list1 = sc.makeRDD(List(1,1,3,4,5,6,"a","b"),5)
    val list2 = sc.makeRDD(List("a","b","d","c",1,1))

//    println(list1.union(list2).collect().toBuffer)
//    println(list1.subtract(list2).collect().toBuffer)
//    println(list1.intersection(list2).collect().toBuffer)
//
//    println(list1.cartesian+(list2).collect().toBuffer)
//
//    println(list1.cartesian(list2).count())

    println(list1.partitions.size)
    list1.map((_,1)).partitionBy(new HashPartitioner(8)).foreach(println(_))

    println(list1.partitions.size)
    println(list1.mapPartitions(l=>Iterator(l.mkString("-"))).collect().toBuffer)






    //关闭资源
    sc.stop()

  }
}
