package SparkGroupFavTeacher

/**
  * 第一种方法适合数据量小的spark处理，得到 grouped 后，之后使用scala的sortBy排序，
  *  /* grouped.mapValues(_.toList.sortBy( -_._2).take(3)) */
  * 需要将迭代器toList到内存中，而内存有限，支持不了海量数据处理，这种方法就是根据filter出相同的学科，
  * 使用spark的sortby再对学科排序，不用担心内存问题,sortby首选
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupFavTeacher4 extends App{

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
  //获取subject的array,记得一定要去重
  private val subjects: Array[String] = subjectAndteacherAndOne.map(_._1._1).distinct().collect()

  //全局聚合,得到(subject,teacher)相同的聚合体，((subject,teacher),N)
  private val reduced: RDD[((String, String), Int)] = subjectAndteacherAndOne.reduceByKey(_+_)
  //对subjects 循环，filter操作将相同的学科搞出来,并且排序

  for(sub <-subjects ){
    val filtered = reduced.filter(_._1._1 == sub)
    val sorted: RDD[((String, String), Int)] = filtered.sortBy(_._2)

    println(sorted.collect.toBuffer)
  }

sc.stop()





}
