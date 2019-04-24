package com.movie
import com.movie.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 构建推荐模型，并将结果存入mysql数据库中
  */
object movieFormat {
  // mysql连接url
  lazy val url = "jdbc:mysql://localhost:3306/movie_model"
  // 用户名
  lazy val username = "test"
  // 密码
  lazy val password = "123456"

  //将title中年份信息单独提取出来；对信息不正确的title进行还原 e.g title="Awfully Big Adventure An " 实际上应该为 "An Awfully Big Adventure"
  def reTitle(originalTitle: String): (String, String) = {
    // 正则表达式，表示分别匹配不包括引号的电影title信息，以及电影年份信息
    val titleAndYearPattern = "[\"]?(.*)(\\(\\d{4}\\)).*".r
    // 为片名和年份赋初值
    var titleNew = originalTitle
    var year = ""
    // 根据正则表达式匹配
    originalTitle match {
      case titleAndYearPattern(ele, str) => {
        titleNew = ele
        year = str
        // 对于包含（）的title，去除括号中的内容
        titleNew = titleNew.replace(findUselessParentheses(titleNew),"")
        var array = titleNew.split(",")
        if (array.size > 0) {
          titleNew = array.reverse.mkString(" ")
        }
        // 对于包含：的，去除冒号后的内容
        titleNew = cutUselessComma(titleNew)
        // 去除句首的空字符串，将多个空格转为一个空格
        titleNew=titleNew.trim().replaceAll(" {2,}", " ")
      }
      case _ => {
//        println(s"               not match $originalTitle")
      }
    }
    (titleNew, year)

  }

  /**
    * 去除标题括号里的内容
    * @param titleNew
    * @return
    */
  def findUselessParentheses(titleNew: String): String = {
    val uselessPattern = ".*(\\(.*\\)).*".r
    var title1 = ""
    titleNew match {
      case uselessPattern(title) =>
        title
      case _ => {
        ""
      }
    }
  }

  /**
    * 将标题中冒号后的内容去掉
    * @param titleNew
    * @return
    */
  def cutUselessComma(titleNew: String): String = {
    val array = titleNew.split(":")
    array(0)
  }

  def main(args: Array[String]) {
    //初始化conf
    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[10]").set("spark.app.id", "sqlTest")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer", "256m")
    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")
    sparkConf.set("spark.akka.frameSize", "500")
    sparkConf.set("spark.rpc.askTimeout", "30")
    val sc = new SparkContext(sparkConf)

    // 从文件数据集中读取movies数据
    val moviesData = sc.textFile("/Users/umeng/bigdata/ml-latest-small/movies.csv")
    // 对电影数据进行ETL，将title进行格式化
    val newMoviesData = moviesData.map(a => {
      println(s"movieName:$a")
      val array = a.split(",")
      if (array.size > 3) {
        (array(0), reTitle(Array.tabulate(array.size-2)(a=>array(a+1)).mkString(","))._1,
          reTitle(Array.tabulate(array.size-2)(a=>array(a+1)).mkString(","))._2, array(array.size-1))
      } else {
        (array(0), reTitle(array(1))._1,reTitle(array(1))._2, array(2))
      }
    }).map(a=>a.toString)

    newMoviesData.collect()
    FileUtil.init("/Users/umeng/bigdata/ml-latest-small/newMoviesSpark.csv")
    newMoviesData.map { movie => {
      var bw = FileUtil.getWriter()
      bw.write(movie + "\n")
    }
    }.collect()

    sc.stop()
  }
}
