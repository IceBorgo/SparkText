package SparkGroupFavTeacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GroupFavTeacher5 extends App{

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
    * 第二种升级版使用自定义分区器，不再将迭代器的内容toList中
    */
  //获得suject的Array
  private val subs: Array[String] = subjectAndteacherAndOne.map(_._1._1).distinct().collect()
  //new 自定义分区器
  private val myPartitions: MyPatition3 = new MyPatition3(subs)
  //全局聚合,得到(subject,teacher)相同的聚合体，((subject,teacher),N)
  private val reduced: RDD[((String, String), Int)] = subjectAndteacherAndOne.reduceByKey(myPartitions,_+_)
  //对每个学科分区进行排序
   private val partitions: RDD[((String, String), Int)] = reduced.mapPartitions(_.toList.sortBy( -_._2).iterator)
  //将分好区后的partitions的元素swap
  private val swapedPartitions2: RDD[(Int, (String, String))] = partitions.map(x=>(x._2,x._1))
  //定义一个treeset


  val result = swapedPartitions2.mapPartitions(its=>{
     val subjectSet = new mutable.TreeSet[(Int, (String, String))]
    for(it <- its){
      if(subjectSet.size<=2){
        subjectSet.add(it)
      }else {
        //比较当前it int值与subjectSet中最小Int比较，subjectSet.head._1 表示取int中最小的int的值
        if (it._1 > subjectSet.head._1) {
          //移除最小Int的set
          subjectSet.remove(subjectSet.head)
          //将it添加到set中
          subjectSet.add(it)
        }
      }
    }
    //由于mapPartitions要求传出迭代器
    subjectSet.iterator
  })

  println(result.collect.toBuffer)
}




class MyPatition3(subs: Array[String]) extends Partitioner {
  private val subMap2 = new mutable.HashMap[String, Int]()
  var i = 0
  for (sub <- subs) {
    subMap2.put(sub, i)
    i += 1
  }

  override def numPartitions: Int = subs.length

  override def getPartition(key: Any): Int = {
    val tea: String = key.asInstanceOf[(String, String)]._1
    subMap2(tea)

  }
}