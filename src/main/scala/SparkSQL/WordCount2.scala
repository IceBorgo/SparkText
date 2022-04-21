package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WordCount2 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("aa").master("local[2]").getOrCreate()

    val line: Dataset[String] = spark.read.textFile("E:\\mrdata\\sparkSql\\b.txt")

    import spark.implicits._
    val words: Dataset[String] = line.flatMap(_.split(" "))

    words.createTempView("t_word")

    val resulted: DataFrame = spark.sql("select value as word,count(*) as counts from t_word group by word order by counts desc")

    println(resulted.show)
  }
}
