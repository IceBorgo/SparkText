package SparkGroupFavTeacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupFavTeacher3 extends App{

  private val conf: SparkConf = new SparkConf().setAppName("GroupFavTeacher1").setMaster("local[4]")

  private val sc: SparkContext = new SparkContext(conf)
  //获取RDD
  private val line: RDD[String] = sc.textFile("E:\\mrdata\\teacher\\input\\teacher.log")
  //对每条数据进行切分，再组合成((subject,teacher),1)
  //   http://bigdata.edu360.cn/laozhang
  val subjectAndteacherAndOne=line.map(line=>{
    val teacher = line.split("/")(3)
    val subject = line.split("/")(2).split("[.]")(0)
    ((subject,teacher),1)
  })

  /**
    * 第二种升级版使用自定义分区器（在reduceBykey里防治分区器），不再将迭代器的内容toList中
    */
  //获得suject的Array
  private val subs: Array[String] = subjectAndteacherAndOne.map(_._1._1).distinct().collect()
  //new 自定义分区器
  private val myPartitions: MyPatition2 = new MyPatition2(subs)
  //全局聚合,得到(subject,teacher)相同的聚合体，((subject,teacher),N)
  private val reduced: RDD[((String, String), Int)] = subjectAndteacherAndOne.reduceByKey(myPartitions,_+_)
  //对每个学科分区进行排序
   private val partitions: RDD[((String, String), Int)] = reduced.mapPartitions(_.toList.sortBy( -_._2).iterator)


  println(partitions.collect.toBuffer)
}

class MyPatition2(subs: Array[String]) extends Partitioner{

  //主构造器里，new1的时候回执行它
  private val subMap2 = new mutable.HashMap[String,Int]()
  var i=0
  for(sub <- subs){
    subMap2.put(sub,i)
    i+=1
  }
  override def numPartitions: Int = subs.length

  override def getPartition(key: Any): Int = {
    val tea: String = key.asInstanceOf[(String,String)]._1
    subMap2(tea)

  }
}