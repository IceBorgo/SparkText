package ExamJedisKafkaSpark

import Exam.Utill
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {
  //计算区域成交金额
  def calculateZone(lines: RDD[String], broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {

    val provinceAndPrice: RDD[(String, Double)] = lines.map(line => {
      val split: Array[String] = line.split(" ")
      val split1 = split(1)
      val ipLong: Long = Utill.ip2Long(split1)
      val price: Double = split(4).toDouble
      //收取广播
      val ipRules: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = Utill.binarySearch(ipRules, ipLong)
      var province = "未知"
      if (index != -1) {
        province = ipRules(index)._3
      }
      (province, price)
    })
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)

    reduced.foreachPartition(its=>{
      val conn = JedisConnectionPool.getConnection()
      its.foreach(x=>{
        conn.incrByFloat(x._1,x._2)
      })
      conn.close()
    })
  }

  def calculateItem(lines: RDD[String]) = {
    val productAndCount: RDD[(String, Double)] = lines.map(line => {
      val split: Array[String] = line.split(" ")
      val product: String = split(2)
      val count = split(4).toDouble
      (product, count)
    })
    val reduced: RDD[(String, Double)] = productAndCount.reduceByKey(_+_)
    reduced.foreachPartition(its=>{
      val conn = JedisConnectionPool.getConnection()
      for(it <-its){
        conn.incrByFloat(it._1,it._2)
      }
      conn.close()
    })


  }


  def productSum(lines: RDD[String]) = {
    //  A 202.106.196.115 手机 iPhone8 8000
    val count: RDD[Double] = lines.map(line => {
      val counts = line.split(" ")(4).toDouble
      counts
    })
    val resulted: Double = count.reduce(_+_)
    //将数据保存到redis中，可以实时更新哦,获取链接
    val conn: Jedis = JedisConnectionPool.getConnection()

    conn.incrByFloat("INCOME",resulted)
    conn.close()


  }

}
