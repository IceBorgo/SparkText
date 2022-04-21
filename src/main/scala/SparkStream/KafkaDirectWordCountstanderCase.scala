package SparkStream

/**
  * 主要思想，启动的时候判断有没有偏移量，没偏移量，从头读，
  * 有偏移量，接着偏移量读，找到这个组，对应的分区(KAFKA的分区)的偏移量是多少，从哪开始读就行了
  *
  *当程序有问题退出时，而kafka还在不断产生数据，改直连方式的程序依旧从之前分区的偏移量读取数据，棒！！
  *
  */

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by zx on 2017/7/31.
  */
object KafkaDirectWordCountstanderCase {

  def main(args: Array[String]): Unit = {
    /**
      * 参数的准备
      */
    //指定组名，每个组可能有不同的topic
    val group = "g001"
    //创建SparkConf
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    //创建SparkStreaming，并设置间隔时间
    val ssc = new StreamingContext(conf, Duration(5000))
    //指定消费的 topic 名字
    val topic = "wwcc"
    //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高，
    // 可以不需要zookeeper了，但因为要将偏移量存储在zookeeper里，所以下面还是写了zookeeper的ip)
    val brokerList = "node-4:9092,node-5:9092,node-6:9092"

    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    val zkQuorum = "node-1:2181,node-2:2181,node-3:2181"
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

    //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)

    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/
    // children 有几个分区，0,1,2分区3个分区
    val children = zkClient.countChildren(zkTopicPath)

    //InputDStream，父类是DStream
    var kafkaStream: InputDStream[(String, String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    /**
      * 程序启动时，开始判断有木有偏移量，有偏移量,取出偏移量
      */
    //如果保存过 offset
    if (children > 0) {
      /**
        * for循环为了得到偏移量，从zookeeper中读取。最后放入Map中，key是TopicAndPartition，values是偏移量
        */
      for (i <- 0 until children) {
        // /g001/offsets/wordcount/0/10001
        // 读这个文件路径/g001/offsets/wordcount/0   获取10001
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0   topic和一个分区放入到fromOffsets这个map中当key，
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001  +=相当于HashMap的put用法
        fromOffsets += (tp -> partitionOffset.toLong)
      }
      /**
        * messageHandler信息的处理方式
        */
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
      //最终(mmd.key(), mmd.message()) 可以只有key，或者values，看自己怎么搞了
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      /**
        * 使用createDirectStream获得kafkaStream
        */
      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder,
        StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    /**
      * 程序启动时，开始判断有木有偏移量，没有偏移量如下程序
      */
    else {
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //偏移量的范围
    var offsetRanges = Array[OffsetRange]()

    /**
      * 第一种和第二种取一个即可
      * 第一种对kafkaStream使用transform，麻烦些，先获取offsetRanges
      */
    //从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    //该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream
    //rdd => 这个rdd是个KAFKARDD（），只是为了取偏移量也只有kafkardd可以获取offsetRanges出来
    //DStream的transform,foreachRDD,都是将DStream里面每时每刻生产的rdd取出来
    val transform: DStream[(String, String)] = kafkaStream.transform { rdd =>
      //得到该 rdd 对应 kafka 的消息的 offset
      //该RDD是一个KafkaRDD，可以获得偏移量的范围,也只有kafkardd可以获取offsetRanges
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val messages: DStream[String] = transform.map(_._2)

    //依次迭代DStream中的RDD，foreachRDD 这个算子可以将DStream每次生产的RDD迭代出来
    //对RDD进行操作，触发Action
    messages.foreachRDD { rdd =>
      //这里就是spark-core的rdd的action操作触发
      //对RDD进行操作，触发Action

      rdd.foreachPartition(partition =>
        partition.foreach(x => {
          println(x)
        })
      )

      /**
        * 第二种直接对kafkaStream进行foreachRDD，获得offsetRanges，简单
        */

      //直连方式只有在KafkaDStream的RDD中才能获取偏移量，那么就不能到调用DStream的Transformation
      //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
      //依次迭代KafkaDStream中的KafkaRDD
      kafkaStream.foreachRDD { kafkaRDD =>
        //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        val lines: RDD[String] = kafkaRDD.map(_._2)
        /**
          * 获得lines后下面就是正常的Spark-core的RDD操作了。
          */
        //对RDD进行操作，触发Action
        lines.foreachPartition(partition =>
          partition.foreach(x => {
            println(x)
          })
        )
        /**
          * 每进行一次rdd的操作，都将从 offsetRanges = kafkaRDD.asInstanceOf
          * [HasOffsetRanges].offsetRanges中获取的offsetRanges偏移量记录到zookeeper中
          */
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

    }


  }
}

