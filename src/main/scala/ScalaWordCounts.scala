import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCounts {

  def main(args: Array[String]): Unit = {

       val conf = new SparkConf().setAppName("ScalaWordCounts").setMaster("local[4]")



    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))

    //创建spark执行的入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    //sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).sortBy(_._2, false).saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reduced:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}
