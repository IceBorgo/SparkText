package ExamUseSparkStreamandKafka

import java.sql.{Connection, DriverManager, PreparedStatement}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}


object KafkaDirect3 {
  def main(args: Array[String]): Unit = {


    //指定组名，每个组可能有不同的topic
    val group = "g001"
    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[3]")
    val sc: SparkContext = new SparkContext(conf)

    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(sc, Duration(5000))
    //指定消费的 topic 名字
    val topic = "exam"

    val brokerList = "master:9092,slave1:9092,slave2:9092"

    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    val zkQuorum = "master:2181,slave1:2181,slave1:2181"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics: Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group, topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )



    val zkClient = new ZkClient(zkQuorum)

    val children = zkClient.countChildren(zkTopicPath)

    //InputDStream，父类是DStream
    var kafkaStream: InputDStream[(String, String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    if (children > 0) {

      for (i <- 0 until children) {

        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0   topic和一个分区放入到fromOffsets这个map中当key，
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001  +=相当于HashMap的put用法
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,
        StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    else {
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    //依次迭代KafkaDStream中的KafkaRDD
    kafkaStream.foreachRDD { kafkaRDD =>
      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines: RDD[String] = kafkaRDD.map(_._2)
      //开始写rdd代码
      // A 202.106.196.115 手机 iPhone8 8000
      //得到(provinceLong, 1)

      val productAndCount= lines.map(line => {
        val split: Array[String] = line.split(" ")
        val product: String = split(2)
        val count: Int = split(4).toInt
        (product,count)
      })
      val resulted: RDD[(String, Int)] = productAndCount.reduceByKey(_+_).sortBy( -_._2)

      //写进mysql里
      resulted.foreachPartition(its=>{
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8","root","123456")
        //如何将Id主键在mysql中传进去呢？？
        val pre: PreparedStatement = conn.prepareStatement("INSERT INTO exam3 VALUES (?,?)")
        for(it <-its){
          pre.setString(1,it._1)
          pre.setInt(2,it._2)
          pre.executeUpdate()
        }
        if(pre != null) {
          pre.close()
        }
        if (conn != null) {
          conn.close()
        }

      })


      for (o <- offsetRanges) {
        //  /g001/offsets/wordcount/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        //将该 partition 的 offset 保存到 zookeeper
        //  /g001/offsets/wordcount/0/20000
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }

    ssc.start()
    ssc.awaitTermination()

    sc.stop()
  }

}
