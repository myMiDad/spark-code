package work

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object ScoreTop2 {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    //sc
    val sc = new SparkContext(conf)
    //读取数据
    val source = sc.textFile(args(0))
    //切分并且过滤

    val filted = source.map(_.split(",",0)).filter(_.length >= 3)
    val rddTuple = filted.map(arr=>(arr(1),(arr(0),arr(2))))
    //拿到班级号
    val keys: Array[String] = rddTuple.map(_._1).distinct().collect()
    //处理数据
    rddTuple.mapPartitions(x=>{
      x.toList.sortBy(_._2._2).reverse.take(2).toIterator
    }).partitionBy(new SortParitioner(keys)).foreach(println(_))
    
    //释放资源
    sc.stop()
  }
}

class SortParitioner(keys: Array[String]) extends Partitioner{
  override def numPartitions: Int = keys.length
  
  //搞一个map用来存储班级号和分区号对应的
  private val mapClassAndPartitioner = new mutable.HashMap[String,Int]()

  var n = 0
  for(key <- keys.sortBy(x=>x.toInt)){
    mapClassAndPartitioner += (key -> n)
    n += 1
  }

  override def getPartition(key: Any): Int = {
    mapClassAndPartitioner.getOrElse(key.toString,0)
  }
}
