package work

import org.apache.spark.{SparkConf, SparkContext}

object ClassJoinScoreBro {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    //sc
    val sc = new SparkContext(conf)
    //读取数据
    val dictMap: collection.Map[String, String] = sc.textFile(args(0)).map(_.split(",", -1)).filter(_.length >= 2).map(arr => (arr(1), arr(0))).collectAsMap()
    //广播变量
    val dictBro = sc.broadcast(dictMap)

    val rdd2 = sc.textFile(args(1)).map(_.split(",", -1)).filter(_.length >= 2).map(arr => (arr(0), arr(1)))

    rdd2.map(tp=>{
      val classID = dictBro.value.getOrElse(tp._1,"0")
      (classID,tp._1,tp._2)
    }).foreach(println(_))

    //释放资源
    sc.stop()
  }
}
