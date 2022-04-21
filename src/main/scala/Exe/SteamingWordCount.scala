package Exe

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SteamingWordCount {

 def main(args: Array[String]): Unit = {

  val conf: SparkConf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[*]")
 //设置流5秒处理一次
  val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

  val zk="master:2181,slave1:2181,,slave1:2181"
  val gIp="g1"
  val topic = Map("xiaoniu" -> 1)

 //消费从kafka来的数据
  val date: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,gIp,topic)

  val key = date.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

  key.print()

  ssc.start()

  ssc.awaitTermination()
 }
}
