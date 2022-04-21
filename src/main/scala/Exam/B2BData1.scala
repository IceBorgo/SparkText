package Exam

/**
  * 问题1.计算出各个省的成交量总额（结果保存到MySQL中）
  */

import java.sql.{Connection, DriverManager, PreparedStatement}

import IpLocationol.MyUtills
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B2BData1 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkIP").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    //获取ipRule
    val ipRule: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\ip.txt")
    //切，得到  (startNum, endNum, province)
    val startNumAndEndNumAndProvince: RDD[(Long, Long, String)] = ipRule.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    //将ipRules聚合collec一下，到driver端
    val ipRules: Array[(Long, Long, String)] = startNumAndEndNumAndProvince.collect()
    //将ipRules广播到executor端
   val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRules)


    /**
      * 问题1.计算出各个省的成交量总额（结果保存到MySQL中）
      */
    //切分原始数据
    //   A 202.106.196.115 手机 iPhone8 8000
    //获取原始数据的rdd
    val lines: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\exam.log")

    //切,得到 (province,1)
    val provinceAndCountsAndOne: RDD[(String, Float)] = lines.map(line => {
      val split: Array[String] = line.split(" ")
      //得到ip地址
      val ipString: String = split(1)
      //得到用户的消费额
    val counts: Float = split(4).toFloat
      val ipLong: Long = Utill.ip2Long(ipString)
      //获取游离在executor的ipRules
      val ipRules = broadcastRef.value
      //二分法查找省
      val index: Int = Utill.binarySearch(ipRules, ipLong)
      val province: String = ipRules(index)._3
      (province, counts)
    })

    //对province聚合得到结果
    val reduced: RDD[(String, Float)] = provinceAndCountsAndOne.reduceByKey(_+_)
    //将数据导入数据库中,数据库该utf-8，不然导不进去
    reduced.foreachPartition(its=>Utill.data2Mysql(its))

    //println(reduced.collect.toBuffer)

      sc.stop()




  }



}
