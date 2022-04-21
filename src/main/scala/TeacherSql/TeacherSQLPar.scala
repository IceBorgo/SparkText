package TeacherSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TeacherSQLPar {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("1").master("local[2]").getOrCreate()

    import spark.implicits._

    //读取teacher文件

    val line: Dataset[String] = spark.read.textFile("E:\\mrdata\\teacher\\input\\teacher.log")
//切数据，获得teaAndSubDF
    val teaAndSubDF: DataFrame = line.map(line => {
      //  http://bigdata.edu360.cn/laozhang
      val split: Array[String] = line.split("/")
      val teacher: String = split(3)
      val subject: String = split(2).split("[.]")(0)
      (teacher, subject)
    }).toDF("teacher", "subject")

    //用sql写
    teaAndSubDF.createTempView("v_tea")
   //使用row_number() over hive的高级用法，分区groupby
    val parResult: DataFrame = spark.sql(
      "select teacher,subject,counts from "+
      "(select teacher,subject,counts ,row_number() over(partition by subject order by counts desc) as rn"+
     " from (select teacher,subject,count(1) counts from v_tea group by teacher,subject) t1) t2 where rn =1 order by counts desc"
    )

    parResult.show()


  }
}
