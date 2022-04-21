package IpLocationol
/**
  *  将ipRules放置hdfs中，在saprk读取， 由于每个executor只读取一部分的ipRule，
    需要将所有的ipRulecollect至driver段，然后在广播到task中
  */
import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkIP2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SparkIP").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

   //获取ipRules的RDD
    val ipRule: RDD[String] = sc.textFile("hdfs://master:9000/sparkdata/ip.txt")
    //切出(start,end,province)
    val startNumAndEndNumAndProvince: RDD[(Long, Long, String)] = ipRule.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    //将ipRule聚合collec一下，到driver端
  val ipRules: Array[(Long, Long, String)] = startNumAndEndNumAndProvince.collect()
    //将聚合好的ipRules广播至executor端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)

    //获取杂乱ip数据的rdd
    val lines: RDD[String] = sc.textFile("hdfs://master:9000/sparkdata/access.log")
    //切
    val provinceAndOne: RDD[(String, Int)] = lines.map(line => {
      val split: Array[String] = line.split("[|]")
      val ipString: String = split(1)
      val ipLong: Long = MyUtills.ip2Long(ipString)
      //程序在executor执行时中获取游离于executor上的ipRules
      val ipRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查询对应ip的index
      val index: Int = MyUtills.binarySearch(ipRules, ipLong)
      val province: String = ipRules(index)._3
      (province, 1)
    })
    //聚合
        val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)
   //写入mysql中，foreachPartition一次拿一个区，减少conn的创建和回收，提高效率
    reduced.foreachPartition(its=>MyUtills.data2Mysql(its))
  sc.stop()
  }
}
