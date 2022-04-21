package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLText2 {

  def main(args: Array[String]): Unit = {
    import org.apache.spark

    import scala.tools.nsc.interpreter.session

    val spark: SparkSession = SparkSession.builder()
      .appName("SQLText2")
      .master("local[2]")
      .getOrCreate()

    val lines: RDD[String] = spark.sparkContext.textFile("E:\\mrdata\\sparkSql\\a.txt")

    val rowRdd: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
    Row(id,name,age,fv)
    })
    val structType: StructType = StructType(List(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("fv", DoubleType)
    ))
    val bdf: DataFrame = spark.createDataFrame(rowRdd,structType)

 /*   bdf.createTempView("boy")

    val sql: DataFrame = spark.sql("select * from boy")

    println(sql.show)*/

    import  spark.implicits._
    val sort: Dataset[Row] = bdf.select("name","age","fv").sort($"fv" desc,$"age" asc)
    println(sort.show)

  }
}
