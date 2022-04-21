package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLText1 {

  def main(args: Array[String]): Unit = {
    import org.apache.spark
    //getOrCreate，如果有就用原来的，没有的话就创建一个
    val session: SparkSession = SparkSession.builder().appName("SQLText1").master("local[2]").getOrCreate()

    val lines: RDD[String] = session.sparkContext.textFile("E:\\mrdata\\sparkSql\\a.txt")

    val rowRdd: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })
    val structType: StructType = StructType(List(
      StructField("id", LongType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("fv", DoubleType)
    ))
    //将表头，表行搞在一起
    val bdf: DataFrame = session.createDataFrame(rowRdd,structType)
   /* bdf.createTempView("boy")
    val resulted: DataFrame = session.sql("select * from boy ")

    println(resulted.show)*/
    //导包
    import session.implicits._
    val sort: Dataset[Row] = bdf.select("name","age","fv").sort($"fv" desc,$"age" asc)
    println(sort.show)

  }
}
