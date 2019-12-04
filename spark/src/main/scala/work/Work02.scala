package work

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * ClassName: Work02
 * Description: 
 *
 * @date 2019/12/2 14:19
 * @author Mi_dad
 */
object Work02 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    val source: RDD[String] = sc.textFile(args(0),1)

    val data: RDD[(String, String, String)] = source.map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2)))

    //1、按照成绩倒序排序
//    data.sortBy(x=>x._3,false,1).foreach(println(_))
    //2、按照班级，然后按照分数倒序
//    val stuRdd: RDD[StudentSort] = data.map(tp=>new StudentSort(tp._1,tp._2.toInt,tp._3.toInt))
//    stuRdd.sortBy(s=>s).foreach(println(_).toString)
    //3、取班级前两名,自定义分区器



    sc.stop()
  }

}
