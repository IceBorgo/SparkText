package sparkHivezhenghe

import org.apache.spark.sql.{DataFrame, SparkSession}


object Demo1 {

  def main(args: Array[String]): Unit = {
     //说没权限啥的，加上这一句就行了
    System.setProperty("HADOOP_USER_NAME","root")
    //如果想让hive运行在spark上，一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()
    //获取权限
    //想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可

    val result: DataFrame = spark.sql("show tables ")
    result.show()
    spark.close()


  }
}
