package work

import org.apache.spark.{SparkConf, SparkContext}

object ClassJoinScore {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    //sc
    val sc = new SparkContext(conf)
    //读取数据
    //读取班级信息表
    val rdd1= sc.textFile(args(0)).map(_.split(",",-1)).filter(_.length>=2).map(arr=>(arr(1),arr(0)))
    rdd1.foreach(println(_))
    //读取学生成绩表
    val rdd2 = sc.textFile(args(1)).map(_.split(",",-1)).filter(_.length>=2).map(arr=>(arr(0),arr(1)))

    rdd1.join(rdd2).map{
      case (name,(classId,score)) => (classId,name,score)
    }.foreach(println(_))

    //释放资源
    sc.stop()
  }
}
