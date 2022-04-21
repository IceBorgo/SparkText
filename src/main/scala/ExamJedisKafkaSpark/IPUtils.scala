package ExamJedisKafkaSpark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object IPUtils {
  def broadcastIpRules(ssc: StreamingContext, ipPath: String):Broadcast[Array[(Long, Long, String)]] = {
    val sc: SparkContext = ssc.sparkContext
    //获取ipRule
    val ipRule: RDD[String] = sc.textFile(ipPath)
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
    broadcastRef
  }


}

