package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object WordCount {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate()

    val lines: RDD[String] = spark.sparkContext.textFile("E:\\mrdata\\sparkSql\\b.txt")
//得到单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
//导入隐式转换包
    import spark.implicits._
    //rdd转成DataFrame
    val wdf: DataFrame = words.toDF()

    import org.apache.spark.sql.functions._
    //聚合函数agg得导上面的包，agg里面放的都是聚合函数
   val resulted: Dataset[Row] = wdf.groupBy($"value" as "word").agg(count("*") as "counts").sort($"counts" desc)


    resulted.show()

  }
}
