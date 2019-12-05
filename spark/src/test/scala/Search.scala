/**
 * ClassName: Search
 * Description: 
 *
 * @date 2019/12/5 19:20
 * @author Mi_dad
 */
object Search {
  def search(arr:Array[Int],num:Int): Int={
    var min = 0
    var max = arr.length-1
    while(min<=max){
      var middle = (max+min)/2
      if (num < arr(middle)) max = middle -1
      else if (num > arr(middle)) min = middle + 1
      else return middle
    }
    return -1
  }
  def main(args: Array[String]): Unit = {
    val arr: Array[Int] = Array(1,2,8,12,23,34,45,65,67,80,13,79,0)
    val index: Int = search(arr.sorted,79)
    if (index != -1) println(arr(index)) else println("不存在该数！")
  }

}
