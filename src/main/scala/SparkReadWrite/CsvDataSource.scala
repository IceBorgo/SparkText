package SparkReadWrite

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    val csv: DataFrame = spark.read.csv("E:\\mrdata\\sparkReadWrite\\csv")

  csv.printSchema()
    csv.show()
  }
}
