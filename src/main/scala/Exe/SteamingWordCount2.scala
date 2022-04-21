package Exe
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object SteamingWordCount2 {

      val updateFunc=(iter: Iterator[(String, Seq[Int], Option[Int])])=>{
        iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))

      }
  def main(args: Array[String]): Unit = {

    //写实时spark程序了
    //  SparkSession.builder().appName().master()这个是sparksql的创建语法
    //离线任务是创建SparkContext，现在要实现实时计算，用StreamingContext
    val conf: SparkConf = new SparkConf().setAppName("12").setMaster("local[*]")
    //创建streamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    //如果要使用课更新历史数据（累加），那么就要把终结结果保存起来
    ssc.checkpoint("E:\\mrdata\\SparkTemp")

    val zk="master:2181,slave1:2181,,slave1:2181"
    val gIp="g001"
    val topic=Map("xiaoniu"->1)

    //从kafkali消费数据，0-8版本需要zookeeper，指定topic
    // ReceiverInputDStream[(String, String)] 也叫kafkaDStream,里面是元祖，
    val date: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zk,gIp,topic)

    //开始写sparkstream程序了，就像写spark-core的rdd一样
    val wordAndOne: DStream[(String, Int)] = date.map(_._2).flatMap(_.split(" ")).map((_, 1))

    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}

