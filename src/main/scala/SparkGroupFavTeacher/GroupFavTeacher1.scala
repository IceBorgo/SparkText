package SparkGroupFavTeacher

/**
  * 第一种方法适合数据量小的spark处理，得到 grouped 后，之后使用scala的sortBy排序，
  *  /* grouped.mapValues(_.toList.sortBy( -_._2).take(3)) */
  * 需要将迭代器toList到内存中，而内存有限，支持不了海量数据处理
  * //下面为实质原因
  * groupBy后，bigData的数据由一个bigData的迭代器引用，
  * 里面没装数据，需要的时候就迭代过去了，所以这造成要排序的话必须引用scala中的toList方法，
  * 但数据量过大的话，就会出现内存不够的尴尬局面，需要自己自定义排序了
  */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 extends App{

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
  //对subject局部聚合， 得到subject->iterotor((subject,teacher),N)
  private val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
  //对N进行排序取前3,将group后得到的迭代器先toList在sortby
  private val result: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy( -_._2).take(3))

  println(result.collect.toBuffer)


}



