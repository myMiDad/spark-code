package work

import org.apache.spark.Partitioner

class MyPartitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = {
    //将key转成想要的类型
    val ckey = key.toString
    //string类型转int
    val ikey = ckey.substring(ckey.length - 1).toInt
    if (ikey % 2 == 0) {
      0
    }else{
      1
    }
  }
}
