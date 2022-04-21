package SparkGroupFavTeacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupFavTeacher2 extends App{

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

  //全局聚合,得到(subject,teacher)相同的聚合体，((subject,teacher),N)
  private val reduced: RDD[((String, String), Int)] = subjectAndteacherAndOne.reduceByKey(_+_)
  /**
    * 第二种使用自定义分区器，只是每个分区的数据都是统一学科的，效率变高，但使用mappartition，还是得将一个分区的数据tolist
    */
    // 取到subject的array
  private val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    //new出自定义的分区器
  private val myPartition: MyPartition = new MyPartition(subjects)
  //自定义分区后，每个分区对应相应的subject
  private val partitiondSubject: RDD[((String, String), Int)] = reduced.partitionBy(myPartition)
  //取出每个学科分区进行排序
  private val result: RDD[((String, String), Int)] = partitiondSubject.mapPartitions(_.toList.sortBy(_._2).iterator)

  println(result.collect().toBuffer)

}


class MyPartition(subs:Array[String]) extends Partitioner{
  private val subMap = new mutable.HashMap[String,Int]()
  var i=0
  for(sub <- subs){
    subMap.put(sub,i)
    i+=1
  }

  override def numPartitions: Int = subs.length

  override def getPartition(key: Any): Int ={
    //(k,v) k为元祖
    val teacher = key.asInstanceOf[(String,String)]._1
    subMap(teacher)
  }
}