package SparkReadWrite

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("1").master("local[2]").getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正mysql的数据吗？
    //不会读取数据，但会建立连接，获取表头信息
    val logs: DataFrame = spark.read.format("jdbc").options(
      //haha是table名
      Map("url" -> "jdbc:mysql://localhost:3306/haha?useUnicode=true&characterEncoding=UTF-8",
        "driver" -> "com.mysql.jdbc.Driver",
        //dbtable，表名字
        "dbtable" -> "ip",
        "user" -> "root",
        "password" -> "123456")
    ).load()
    //打印表头
    //logs.printSchema()

   // logs.show()

   // val filtered: Dataset[Row] = logs.filter(r=>{r.getAs[Int]("count")<=1000})
   // filtered.show()

    //val filtered: Dataset[Row] = logs.filter($"count" <1000)
    //filtered.show()

    val select: DataFrame = logs.select("*")
    select.printSchema()
   // select.show()
   //加上utf-8,妈的，数据库的utf8没改
    val props = new Properties()
    props.put("user","root")
    props.put("password","123456")
    select.write.mode("ignore")
      .jdbc("jdbc:mysql://localhost:3306/ttt?useUnicode=true&characterEncoding=UTF-8", "ip12", props)

   // select.write.text("E:\\mrdata\\sparkReadWrite\\text")

    //select.write.json("E:\\mrdata\\sparkReadWrite\\json")

    //select.write.csv("E:\\mrdata\\sparkReadWrite\\csv")

    //select.write.parquet("E:\\mrdata\\sparkReadWrite\\parquet")
  }
}
