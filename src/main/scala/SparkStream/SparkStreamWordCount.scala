package SparkStream

/**
  * 不知为啥报错，哎，依赖导的有问题
  */

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object SparkStreamWordCount {

  def main(args: Array[String]): Unit = {


   val conf: SparkConf = new SparkConf().setAppName("as").setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc,Milliseconds(5000))

    val line: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.9.12",8888)

    val reslut: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    reslut.print()

    ssc.start()

    ssc.awaitTermination()


  }
}
