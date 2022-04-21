package SparkSQLUpdate

/**
  *  jon的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
  *  1.将iprule收集到driver端，然后广播出去
  *  2.切ip数据，得到ip的DF
  *  3.自定义sql规则，将ip转为province，传到sql里
  *  4.写sql
  */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkIpYuoHua {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("11").master("local[1]").getOrCreate()

    import spark.implicits._

    val lines: Dataset[String] = spark.read.textFile("E:\\mrdata\\ip\\ip.txt")
    //切,得到iprule的dataset
    val ipRule: Dataset[(Long, Long, String)] = lines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    //收集起来，广播出去
    val ipruleArr: Array[(Long, Long, String)] = ipRule.collect()
    //广播，每个executor就会收到这个东东
    val ipruleInDriver: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(ipruleArr)


    //读ip数据
    val line: Dataset[String] = spark.read.textFile("E:\\mrdata\\ip\\access.log")
    //切,得到ipDF
    val ipDF: DataFrame = line.map(line => {
      val split: Array[String] = line.split("[|]")
      val ipString: String = split(1)
      val ipLong = MyUtills.ip2Long(ipString)
      ipLong
    }).toDF("ip")

    //制定一条规则，将输入ip输出province
    spark.udf.register("ip2province",(ip:Long)=>{
        //把游离的于excutor端的规则获取
        val ipruleInExcutor: Array[(Long, Long, String)] = ipruleInDriver.value
        //调二分法获取index
        val index: Int = MyUtills.binarySearch(ipruleInExcutor,ip)
        var province:String="未知"
        if(index != -1){
          province= ipruleInExcutor(index)._3
        }
        province
    })

    //写sql
    ipDF.createTempView("v_ip")
    //将规则搞进sql语句中
    val resulted: DataFrame = spark.sql("select ip2province(ip) province, count(1) counts from v_ip group by province order by counts desc")

   resulted.show(10)

    spark.stop()
  }
}


