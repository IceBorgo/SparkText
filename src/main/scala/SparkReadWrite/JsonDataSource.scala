package SparkReadWrite

import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val json: DataFrame = spark.read.json("E:\\mrdata\\sparkReadWrite\\json")

    json.printSchema()

    json.show()
  }
}
