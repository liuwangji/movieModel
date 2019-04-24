package com.movie

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

object CreateModelForExperiment {
  lazy val url = "jdbc:mysql://localhost:3306/movie_model"
  lazy val username = "test"
  lazy val password = "123456"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[2]").set("spark.app.id", "sqlTest")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer", "256m")
    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")
    sparkConf.set("spark.akka.frameSize", "500")
    sparkConf.set("spark.rpc.askTimeout", "30")

    val sc = new SparkContext(sparkConf)
    // 从文件数据集中读取原始数据
    val movieData = sc.textFile("/Users/umeng/Documents/bigdata/ml-latest-small/ratings.csv")
    // 对原始数据进行清洗，得到用户、电影、评分矩阵
    val ratings = movieData.map(_.split(',') match { case Array(user, item, rate, timeStamp) => Rating(user.toInt, item.toInt, rate.toDouble) })
    // 80%数据作为训练集，20%数据作为测试集   可以改变比例或是用交叉验证的方法
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    // 构建推荐模型
    val rank = 10
    val numIterations = 10
    val model = ALS.train(training, rank, numIterations, 0.01)
    // 用模型对测试集中的数据进行预测，并与实际值进行对比
    val usersProducts = test.map { case Rating(user, product, rate) => (user, product) }
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate) }
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate) }.join(predictions)
    // 评价模型准确率--方根误差
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    // 评价模型准确率--相对误差
    val MRE = ratesAndPreds.map { case ((user, product), (r1, r2)) => val err1 = ((r1 - r2) / r1).abs
      err1
    }.mean()
  }

}