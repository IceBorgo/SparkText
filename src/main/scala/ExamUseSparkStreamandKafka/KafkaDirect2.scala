package ExamUseSparkStreamandKafka

import java.sql.{Connection, DriverManager, PreparedStatement}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.collection.mutable
object KafkaDirect2 {
  def main(args: Array[String]): Unit = {


    //指定组名，每个组可能有不同的topic
    val group = "g001"
    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[3]")
    val sc: SparkContext = new SparkContext(conf)

    /**
      * 得到iprule,然后广播出去
      */
    //获取ipRule
    val ipRule: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\ip.txt")
    //切，得到  (startNum, endNum, province)
    val startNumAndEndNumAndProvince: RDD[(Long, Long, String,String)] = ipRule.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      val city=fileds(7)
      (startNum, endNum, province,city)
    })
    //将ipRules聚合collec一下，到driver端
    val ipRules: Array[(Long, Long, String,String)] = startNumAndEndNumAndProvince.collect()
    //将ipRules广播到executor端
    val ipRulesbroadcastRef: Broadcast[Array[(Long, Long, String,String)]] = sc.broadcast(ipRules)



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

      val provinceAndCityAndOne= lines.map(line => {
        val provinceString: String = line.split(" ")(1)
        val provinceLong: Long = UtillUseExamSparkKfka.ip2Long(provinceString)
        val counts: Int = line.split(" ")(4).toInt
        val ipRules: Array[(Long, Long, String,String)] = ipRulesbroadcastRef.value
        val index: Int = UtillUseExamSparkKfka.binarySearch2(ipRules,provinceLong)
        val province: String = ipRules(index)._3
        val city: String = ipRules(index)._4
        ((province, city),counts)
      })
       //得到provinces省份集合供后面自定义分区器使用
       val provinces: Array[String] = provinceAndCityAndOne.map(_._1._1).distinct().collect()
     //经过此步，一个分区就为一个省
      val reduced= provinceAndCityAndOne.reduceByKey(MyPartition(provinces),_+_)
      //使用mapPartitions,对counts进行排序
      val grouped: RDD[((String, String), Int)] = reduced.mapPartitions(_.toList.sortBy( -_._2).iterator)

      //写进mysql里
      grouped.foreachPartition(its=>{
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?characterEncoding=UTF-8","root","123456")
        //如何将Id主键在mysql中传进去呢？？
        val pre: PreparedStatement = conn.prepareStatement("INSERT INTO exam2 VALUES (?,?,?)")
        for(it <-its){
          pre.setString(1,it._1._1)
          pre.setString(2,it._1._2)
          pre.setInt(3,it._2)
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



//自定义分区器，每个区就为一个省
case class MyPartition(pros:Array[String]) extends Partitioner{
  private val proMap = new mutable.HashMap[String,Int]()
  var i=0
  for(pro <- pros){
    proMap.put(pro,i)
    i+=1
  }

  override def numPartitions: Int = pros.length

  override def getPartition(key: Any): Int ={
    //(k,v) k为元祖
    val teacher = key.asInstanceOf[(String,String)]._1
    proMap(teacher)
  }
}
