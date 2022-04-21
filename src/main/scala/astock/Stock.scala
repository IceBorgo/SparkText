package astock

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ 2018/2/28 Liffett 14:38
  *
  */

object Stock {

  def main(args: Array[String]): Unit = {

  val spark: SparkSession = SparkSession.builder()
      .appName("Stock")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val fileRDD= sc.textFile("E:\\mrdata\\APollo\\stock")
    val fileDF: DataFrame = fileRDD.map(line => {
      val split: Array[String] = line.split("\t")
      (split(0), split(1), split(7), split(8), split(9))
    }).toDF("代码", "名称", "涨幅", "跌幅", "反弹")


    fileDF.createTempView("stock")

    val sql: DataFrame = spark.sql("select * from stock")

    sql.show(100)

  }

}
