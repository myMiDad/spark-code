/**
 * ClassName: Test
 * Description: 
 *
 * @date 2019/11/29 19:59
 * @author Mi_dad
 */
object Test {
  def main(args: Array[String]): Unit = {
    val studentsList = List(("张三","男",1998),("李四","女",1997),("张三","男",1999),("李四","女",1996),("张三","男",1993),("李四","女",2000))
    var boyList = List[Any]()
    var girlList = List[Any]()
    var girlgt18List = List[Any]()
    var top3List = List[Any]()
    var girltop3List = List[Any]()
    studentsList.foreach(stu => {
      if(stu._2 == "男") boyList = boyList :+ stu else if(stu._2=="女") girlList = girlList :+ stu
      if(stu._2 == "女" && (2019 - stu._3)>=18) girlgt18List = girlgt18List :+ stu
    })
    var i = 0
    var j = 0

    val tuples: List[(String, String, Int)] = studentsList.sortBy(stu=>(stu._3))
    tuples.foreach(stu=>{
      if (i<3) top3List = top3List :+ stu
      i+=1
      if (j<3 && stu._2 == "女") girltop3List = girltop3List :+ stu
      j+=1
    })

//    var boyList = List[Any]()
//    var girlList = List[Any]()
//    var girlgt18List = List[Any]()
//    var top3List = List[Any]()
//    var girltop3List = List[Any]()
    println("---boyList---")
    boyList.foreach(println(_))
    println("---girlList---")
    girlList.foreach(println(_))
    println("---girlgt18List---")
    girlgt18List.foreach(println(_))
    println("---top3List---")
    top3List.foreach(println(_))
    println("---girltop3List---")
    girltop3List.foreach(println(_))


  }

}
