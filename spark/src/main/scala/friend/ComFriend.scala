package friend

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * ClassName: ComFriend
 * Description: 
 *
 * @date 2019/12/2 21:02
 * @author Mi_dad
 */
object ComFriend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //将数据中每行数据 A B,C,D,E,F,G,H,M变为：一个个元组：(B,A),(C,A),...
    val source: RDD[String] = sc.textFile(args(0))
    val mapData1 = source.flatMap(x => {
      val infos = x.split(" ")
      val user = infos(0)
      val friends = infos(1).split(",")
      val tuples = friends.map(friend => (friend, user))
      tuples
    })
    //使用reduce聚合
    val reduceData1 = mapData1.reduceByKey((x,y)=>x.concat("-").concat(y))

    //执行第二个map
    val mapData2 = reduceData1.flatMap(tp => {
      val friend = tp._1
      val users = tp._2.split("-", -1)
      if (users.length >= 0) {

      }
      val usersNew = users.sortBy(u => u)
      val list = ListBuffer[String]()
      for (i <- 0 until usersNew.size - 1) {
        for (j <- i + 1 until usersNew.size) {
          list.append(usersNew(i) + "-" + usersNew(j))
        }
      }
      val tuples = list.map(k => (k, friend))
      tuples
    })
    //执行第二个reduce
    val reduceData2 = mapData2.reduceByKey((x,y)=>x.concat(",").concat(y))

    reduceData2.saveAsTextFile(args(1))

    sc.stop()
  }

}
