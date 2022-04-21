package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}


object SQLDemo2 {

  def main(args: Array[String]): Unit = {
    import org.apache.spark
    /**
      * 1.X版本
      */
    val conf: SparkConf = new SparkConf().setAppName("firstSql").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    //需要new一个SQLContext
    val sqlContext: SQLContext = new SQLContext(sc)
   //依旧用sc读取数据
    val lines: RDD[String] = sc.textFile("E:\\mrdata\\sparkSql\\a.txt")
    //得到结构化数据boyRdd
    val rowRdd = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      //本次不使用样本例，使用row，但row需要结合
     Row(id, name, age, fv)
    })
    //有行数据row，需要定义下表头
      val structType: StructType = StructType(List(
        //true 是不是可以为空,LongType等包名记得导spark.sql的包
        StructField("id", LongType, true),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("fv", DoubleType)
      ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRdd,structType)
    //变成DF后就可以使用两种API进行编程了
    //把DataFrame先注册临时表
    //第一种用sql写,先注册临时表
   /* bdf.registerTempTable("t_boy")
    val resulted: DataFrame = sqlContext.sql("select * from t_boy order by fv desc,age asc")
    println(resulted.show(3))*/

    //第二种，用usl写，就不需要注册,记得导sql导入隐式转换
    import sqlContext.implicits._
    val resulted: Dataset[Row] = bdf.select("name","age","fv").orderBy($"fv" desc,$"age" asc)
println(resulted.show())



  }
}

//样本例实现了序列化接口，而且不需要new
//case class Boy(id:Long,name:String,age:Int,fv:Double)