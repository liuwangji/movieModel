package com.movie

import com.movie.util.FileUtil
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 构建推荐模型，并将结果存入mysql数据库中
  */
object CreateModel {
  // mysql连接url
  lazy val url = "jdbc:mysql://localhost:3306/movie_model"
  // 用户名
  lazy val username = "test"
  // 密码
  lazy val password = "123456"

  def reTitle(title: String): (String, String) = {
    val pattern1 = "[\"]?(.*)(\\(\\d{4}\\)).*".r
    var titleNew = ""
    var year = ""
    println(title)
    title match {
      case pattern1(ele, str) => {
        titleNew = ele
        year = str
        var array = title.split(",")
        if (array.size > 0) {
          titleNew = array.reverse.mkString(" ")
        }
        println(s"$titleNew,$year")
      }
      case _ => println("not match")
    }
    return (titleNew, year)
  }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[10]").set("spark.app.id", "sqlTest")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer", "256m")
    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")
    sparkConf.set("spark.akka.frameSize", "500")
    sparkConf.set("spark.rpc.askTimeout", "30")
    val sc = new SparkContext(sparkConf)
    // 从文件数据集中读取原始数据
    val ratingData = sc.textFile("/Users/umeng/bigdata/ml-latest-small/ratings.csv")

//        newMoviesData.saveAsTextFile("/Users/umeng/bigdata/ml-latest-small/newMoviesSpark.csv")
        // 对原始数据进行清洗，得到用户、电影、评分矩阵
        val ratings = ratingData.map(_.split(',') match { case Array(userId, movieId, rating, timeStamp) => Rating(userId.toInt, movieId.toInt, rating.toDouble) })
        // 全部数据用来建模
        val training = ratings
        // 构建推荐模型
        val rank = 10
        val numIterations = 10
        val model = ALS.train(training, rank, numIterations, 0.01)
        // 找到最大的用户Id
        val maxUserId = training.map { case Rating(userId, movieId, rating) => userId.toInt }.max()
        // 创建全量用户集合RDD
        val users = sc.parallelize(Range(1, maxUserId))
        // 找到最大的movieId
        val maxMovieId = training.map { case Rating(userId, movieId, rating) => movieId.toInt }.max()
        // 创建全量电影集合RDD
        val movies = sc.parallelize(Range(1, maxMovieId))
        // 得到用户电影的笛卡尔乘积
        val userMovies = users.cartesian(movies)
        // 根据模型对全量的数据集进行评分预测
        val predictionRating = model.predict(userMovies)

        println(s"total sum = ${predictionRating.count()}")
        println(s"distinct sum = ${predictionRating.distinct().count()}")
//        predictionRating.saveAsTextFile("/Users/umeng/Documents/bigdata/ml-latest-small/predict.csv")
//     将结果分批写入文件
        FileUtil.init("/Users/umeng/Documents/bigdata/ml-latest-small/newpredict.csv")
        predictionRating.map { case Rating(userId, movieId, rating) => {
          var bw = FileUtil.getWriter()
          bw.write(userId + "," + movieId + "," + rating + "\n")
        }
        }.collect()
    // 将结果分批写入mysql
    //    predictionRating.mapPartitions(partition => Iterator {
    //      var conn = JdbcUtil.getConn()
    //      var sql = "insert into predict(userId, movieId, rating) values(?,?,?)";
    //      var psmt = conn.prepareStatement(sql);
    //      while (partition.hasNext) {
    //        psmt.setInt(1, partition.next().user);
    //        psmt.setInt(2, partition.next().product);
    //        psmt.setDouble(3, partition.next().rating);
    //        psmt.addBatch()
    //      }
    //      psmt.executeUpdate();
    //      psmt.close();
    //    }).collect()
    //    JdbcUtil.getConn.close()
    sc.stop()
  }
}