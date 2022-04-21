package IpLocationol

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将ipRule放置本地中,ip杂乱数据放置hdfs中，本人觉得该方法没有啥工业价值。
  */
object SparkIP1 {
  def main(args: Array[String]): Unit = {
    val ipRules: Array[(Long, Long, String)] = MyUtills.readRules("E:\\java资料\\spark-1\\spark安装\\ip\\ip.txt")

    val conf: SparkConf = new SparkConf().setAppName("SparkIP1").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    //将本地的ipRule广播到每个executor端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)
//获取rdd
    val lines: RDD[String] = sc.textFile("hdfs://master:9000/sparkdata/access.log")
    //切,获取ip,获取游离于executor上的ipRule,调用二分法查出ip对应的index，得出(province,1)
        val provinceAndOne: RDD[(String, Int)] = lines.map(line => {
          val split: Array[String] = line.split("[|]")
          val ipString: String = split(1)
          val ipLong: Long = MyUtills.ip2Long(ipString)
          //在executor端执行时，获取游离于executor端的ipRule
          val value: Array[(Long, Long, String)] = broadcastRef.value
          //二分法，查询ip对应的index
          val index: Int = MyUtills.binarySearch(value, ipLong)
          val province: String = value(index)._3
          (province, 1)
        })
    //聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    println(reduced.collect.toBuffer)
    sc.stop()
  }
}
