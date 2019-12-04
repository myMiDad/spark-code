import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Transformations {
  def main(args: Array[String]): Unit = {
    //创建rdd
    val conf = new SparkConf().setAppName("Transformations").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 3, 3, 6, 7, 8))
    val rdd1: RDD[String] = sc.makeRDD(List("wo shi bei jing ren", "wo shi shang hai ren"))
    //map 映射，可以理解成带返回值的for循环，map（func） func指定代码逻辑
    //数乘以2+数除以5
    rdd.map(x => (x * 2 + x * 1.0 / 5))

    //filter 过滤，可以理解为带返回值的for循环，返回的值和原值相同
    // 函数中放一个表达式，表达式为true，则保留，如果是false不保留
    //表达式中必须包含输入数据
    rdd.filter(_ % 2 == 0)

    //flatMap(func)  可以理解成遍历循环数据，将每个数据按照自己的格式进行处理
    //并且脱掉内层关系  func 表示代码逻辑
    rdd1.flatMap(_.split(" "))

    //处理数据 rdd  一组切片
    //mapPartitions  用来做分区处理，先拿出一个区来  处理完 在处理另外的区
    //往往应用到使用第三方库  mysql,一个分区获取一个线程，每个分区有一个线程
    //并行计算
    rdd.mapPartitions(partition => {
      val connection = DriverManager.getConnection("jdcb:mysql://localhost:3306/test", "root", "root")
      val ints = partition.map(x => {
        //业务处理
        x
      })
      connection.close()
      ints
    })

    //mapPartitionsWithIndex(func)  操作分区，并且传入一个函数  函数的输入他是分区号数据
    //返回值应该是一个新的结果
    //(Int, Interator[T]) => Iterator[U]
    val rdd2 = sc.parallelize(List(("kpop","female"),("zorro","male"),("mobin","male"),("lucy","female")))
    //定义一个函数，用来传入mapPartitionsWithIndex（）中
    //函数的要求是参数是分区号和迭代器
    //返回值要求是一个迭代器
    def partitionFun(index:Int,iterator:Iterator[(String,String)]):Iterator[(String,String)]={
      //定义一list用来存放姓名+分区号
      var women = List[(String,String)]()
      //判断是否有数据
      while (iterator.hasNext){
        //获取数据
        val tp: (String, String) = iterator.next()
        //判断是否为女性
        tp match {
          case (_,"female") => women :+= (index.toString,tp._1)
          case _ => women :+= (index.toString,"0")
        }
      }
      return women.iterator
    }

    rdd2.mapPartitionsWithIndex(partitionFun)


    //distinct（numtasks） 去重操作  numtask申请多少个任务来执行，
    //默认启动一个task，但是distinct在工作中不建议使用，造成shufffle效率极低，借助第三方库
    rdd.distinct(8)


    //coalesce  修改分区数  分区数变少的时候建议用coalesce
//    println(rdd.partitions.size)
    //val rddPatition = rdd.coalesce(3)
    //println(rddPatition.partitions.size)

    //repatition  修改分区数    分区数变多的时候建议用repatition
//    val rddPatition = rdd.repartition(4)
    //println(rddPatition.partitions.size)


    //sortby (func,排序方式，任务数)
    //后两个参数是可以不用写的  不写的话使用默认值  默认值是升序 和  分区数对于线程数
    rdd.sortBy(x=>x,false,4)

    //union()  并集，括号中加其他的rdd ,不去重
    rdd.union(rdd)

    //subtract()  差集    subtract左面有的  括号中没有的保留
    rdd.subtract(sc.makeRDD(List[Int](1,5,9,0)))

    //intersection  交集  并且去重复
    rdd.intersection(sc.makeRDD(List[Int](1,3,9,0))).foreach(println(_))

    //cartesian()  笛卡尔积  不建议使用
    //println(rdd.cartesian(rdd).count())


    //parititionby   传入一个分区器，分区器就是一个分区规则
    //coalesce 和repartition  传入分区的数量
    //partitionby 是针对于k-v类型
    //patitioner  默认有两种  hashparitioner  rangepatitioner
    rdd2.partitionBy(new HashPartitioner(3))


    //join()   括号中存放的是rdd  rdd要求是（k,v） 按照key进行聚合
    //得到的结果是 （key，（join之前的rdd的value，join之后的rdd的value））
    val rdd3 = sc.parallelize(List(("kpop",25),("zorro",30),("mobin",15)))
    rdd2.join(rdd3)

    //reduceByKey()   括号中存放的func函数   但是左面的rdd要求是k-v类型
    val rdd4: RDD[(String, Int)] = sc.parallelize(List(("kpop",25),("apop",30),("apop",15)))
    rdd4.reduceByKey(_+_)

    //groupByKey   按照key进行聚合
    rdd4.groupByKey().mapValues(tp=>tp.sum)

    //sortByKey([ascending], [numTasks])   参数为排序方式   task数量  如果不传使用默认值，升序 task数量等于线程数
    rdd4.sortByKey(true,1)

    //mapValues  操作数据  k  v
    rdd4.groupByKey().mapValues(t=>t.sum)







    //释放资源
    sc.stop()

  }
}
