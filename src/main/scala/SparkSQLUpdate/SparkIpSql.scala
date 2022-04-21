package SparkSQLUpdate

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkIpSql {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("11").master("local[2]").getOrCreate()
    import spark.implicits._
    //读取IP规则
    val lines: Dataset[String] = spark.read.textFile("E:\\mrdata\\ip\\ip.txt")
    //切


    val ipRule: Dataset[(Long, Long, String)] = lines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    //得到iprule表
    val ipruleDF: DataFrame = ipRule.toDF("sNm","eNM","province")

//读ip数据表
    val line2: Dataset[String] = spark.read.textFile("E:\\mrdata\\ip\\access.log")
    //切，得到ip地址
        val map: Dataset[(Long, String)] = line2.map(line => {
          val split: Array[String] = line.split("[|]")
          val ipString: String = split(1)
          val split1 = split(2)
          val ipLong = MyUtills.ip2Long(ipString)
          (ipLong, split1)
        })

    map.show()

   /* //转为datafram表
     val ipDF: DataFrame = ipLongs.toDF("ipLong")

     ipDF.createTempView("t_ip")
     ipruleDF.createTempView("t_ipRule")

    val result: DataFrame = spark.sql("select province,count(*) counts from t_ipRule join t_ip   on (ipLong>sNm and ipLong<eNM) group by province")

   // result.show(10)*/

  }
}
