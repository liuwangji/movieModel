package main.scala.com.movie
object test {
  def reTitle(title: String): (String, String) = {
    //正则表达式，表示分别匹配不包括引号的电影title信息，以及电影年份信息
    val titleAndYearPattern = "[\"]?(.*)(\\(\\d{4}\\)).*".r

    var titleNew = title
    var year = ""
    title match {
      case titleAndYearPattern(ele, str) => {
        titleNew = ele
        year = str
        // 对于包含（）的title，去除括号中的内容
        titleNew = titleNew.replace(findUseless(titleNew), "")
        var array = titleNew.split(",")
        if (array.size > 0) {
          // 去除句首的空字符串，将多个空格转为一个空格
          titleNew = array.reverse.mkString(" ").trim().replaceAll(" {2,}", " ")
        }
        println(s"       title:$title")
        println(s"$titleNew,$year")

      }
      case _ => {
        println(s"not match $title")
      }
    }
    (titleNew, year)

  }

  def findUseless(titleNew: String): String = {
    val uselessPattern = ".*(\\(.*\\)).*".r
    var title1 = ""
    titleNew match {
      case uselessPattern(title) =>
        println(s"       error:$title")
        title
      case _ => {
        println(s"not match useLess  $titleNew")
        ""
      }
    }
  }

  def main(args: Array[String]): Unit = {
    reTitle("Hidden Fortress, The (Kakushi-toride no san-akunin) (1958)")
    var movie = "8035,Stendhal Syndrome, The (Sindrome di Stendhal, La) (1996),Crime | Horror | Thriller"
    val array = movie.split(",")
      (array(0), reTitle(Array.tabulate(array.size-2)(a=>array(a+1)).mkString(","))._2, array(array.size-1))
  }
}
