package teacher

import org.apache.spark.{SparkConf, SparkContext}

/**
 * ClassName: TestcherScala
 * Description: 
 *
 * @date 2019/12/2 22:13
 * @author Mi_dad
 */
object TestcherScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val source = sc.textFile(args(0))
    source




    sc.stop()
  }

}
