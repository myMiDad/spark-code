package sparksql

import org.apache.spark.sql.SparkSession

/**
 * ClassName: SqlFileRead
 * Description:
 *
 * @date 2019/12/4 10:54
 * @author Mi_dad
 */
object SqlFileRead {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    import session.implicits._

    val frame = session.read.textFile("C:\\Users\\Administrator\\Desktop\\business.txt")
    frame.map(line=>{
      val splits: Array[String] = line.split(",")
      (splits(0),splits(1),splits(2))
    }).show()



    session.stop()
  }

}
