package work

/**
 * ClassName: StudentSort
 * Description: 
 *
 * @date 2019/12/2 14:36
 * @author Mi_dad
 */
class StudentSort(val name: String, val cla: Int, val score: Int) extends Ordered[StudentSort] with Serializable  {

  override def compare(that: StudentSort): Int = {
    if (this.cla == that.cla) {
      that.score - this.score
    } else {
      that.cla - this.cla
    }
  }

  override def toString: String = s"$name,$cla,$score"
}
