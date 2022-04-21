package TeacherSql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SqlallTeacher {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("1").master("local[2]").getOrCreate()

    import spark.implicits._

    //读取teacher文件

    val line: Dataset[String] = spark.read.textFile("E:\\mrdata\\teacher\\input\\teacher.log")

    //切，得到teacherDF
    val teacherDF: DataFrame = line.map(line => {
      //  http://bigdata.edu360.cn/laozhang
      val split: Array[String] = line.split("/")
      val teacher: String = split(3)
      teacher
    }).toDF("teacher")

    //用dsl写sql
    /*import org.apache.spark.sql.functions._
    val result: DataFrame = teacherDF.groupBy("teacher").agg(count("*") as "counts").sort($"counts" desc)*/

    //用sql写
    teacherDF.createTempView("v_teacher")

    val result: DataFrame = spark.sql("select teacher,count(1) counts from v_teacher group by teacher order by counts desc ")

   // result.show()
    //写进数据库

    val props = new Properties()
    props.put("user","root")
    props.put("password","123456")
    result.write.mode("ignore")
      .jdbc("jdbc:mysql://localhost:3306/spark?useUnicode=true&characterEncoding=UTF-8", "topNTeacher1", props)

    spark.stop()
  }
}
