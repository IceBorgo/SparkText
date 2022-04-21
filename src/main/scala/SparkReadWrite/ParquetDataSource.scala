package SparkReadWrite

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()

    val parquet: DataFrame = spark.read.parquet("E:\\mrdata\\sparkReadWrite\\parquet")

    parquet.printSchema()

    parquet.show()
  }
}
