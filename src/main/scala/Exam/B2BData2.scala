package Exam

/**
  * 问题2.计算每个省城市成交量的top3（结果保存到MySQL中）
  * 还需优化，自定义分区器，mysql链接过多，效率低
  *  //放进mysql中(不能用mapPartition，conn链接过多，效率低，上面应该使用自定义分区器分区，排序)
  */

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object B2BData2 {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkIP").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    //获取ipRule
    val ipRule: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\ip.txt")
    //切，得到  (startNum, endNum, province,city)
    val startNumAndEndNumAndProvince: RDD[(Long, Long, String, String)] = ipRule.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      val city = fileds(7)
      (startNum, endNum, province, city)
    })
    //将ipRules聚合collec一下，到driver端
    val ipRule2: Array[(Long, Long, String, String)] = startNumAndEndNumAndProvince.collect()
    //将ipRules广播到executor端
    val broadcastRef = sc.broadcast(ipRule2)

    /**
      * 问题2.计算每个省城市成交量的top3（结果保存到MySQL中）
      */

    val lines: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\exam.log")
    //切，获取 ((province, city), counts)
    val ProvinceAndCityAndCounts: RDD[((String, String), Float)] = lines.map(line => {
      //切分原始数据
      //   A 202.106.196.115 手机 iPhone8 8000
      val split: Array[String] = line.split(" ")
      //得到ip地址
      val ipString: String = split(1)
      //得到用户的消费额
      val counts: Float = split(4).toFloat
      val ipLong: Long = Utill.ip2Long(ipString)
      //获取游离在executor的ipRules
      val ipRule2 = broadcastRef.value
      //二分法查省市，获取脚标
      val index: Int = Utill.binarySearch2(ipRule2, ipLong)
      val province: String = ipRule2(index)._3
      val city: String = ipRule2(index)._4
      ((province, city), counts)
    })
    //对(province, city)，相同的聚合
    val reduced: RDD[((String, String), Float)] = ProvinceAndCityAndCounts.reduceByKey(_ + _)
    //获取省的array，为下面排序准备
    val provinces: Array[String] = ProvinceAndCityAndCounts.map(_._1._1).distinct().collect()
    //
    for (pro <- provinces) {
      //拿出相同的省出来
      val filtered: RDD[((String, String), Float)] = reduced.filter(_._1._1 == pro)
      //对countd倒序排序，map成( x,y , z)格式，取前三
      val result: Array[(String, String, Float)] = filtered.sortBy(-_._2).map(x => (x._1._1, x._1._2, x._2)).take(3)
      // println(resulted.toBuffer)

      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/haha?characterEncoding=UTF-8", "root", "123456")
      //放进mysql中(不能用mapPartition，conn链接过多，效率低，上面应该使用自定义分区器分区，排序)
      // 把conn提上去不就行了？？？
      result.foreach(it => {
        //如何将Id主键在mysql中传进去呢？？
        val pre: PreparedStatement = conn.prepareStatement("INSERT INTO exam2 VALUES (?,?,?)")
        pre.setString(1, it._1)
        pre.setString(2, it._2)
        pre.setFloat(3, it._3)
        pre.executeUpdate()
        if (pre != null) {
          pre.close()
        }
        if (conn != null) {
          conn.close()
        }
      })
    }
    sc.stop()
  }
}



