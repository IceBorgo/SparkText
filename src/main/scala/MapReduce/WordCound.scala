package MapReduce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCound extends App{

  private val conf: SparkConf = new SparkConf().setAppName("WordCound").setMaster("local[1]")

  private val sc: SparkContext = new SparkContext(conf)
  /*
  //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
  private val line: RDD[String] = sc.textFile("E:\\mrdata\\wordcount\\input\\a.txt")
  line.flatMap(_.split((" "))).map((_,1)).reduceByKey(_+_).sortBy( -_._2).saveAsTextFile("E:\\mrdata\\wordcount\\output")*/


  private val line: RDD[String] = sc.textFile("E:\\mrdata\\top\\input\\request.dat")
  //line.map(_.split(" ")(1)).map((_,1)).reduceByKey(_+_).sortBy( -_._2).saveAsTextFile("E:\\mrdata\\top\\output")
//    2017/07/28 qq.com/a


  //求出每个网站被访问次数最多的top3个url《分组TOPN》
  //切成(网页,（url，1))的格式
    /* line.map(_.split(" ")(1)).map(_.split("/")).map(x=>((x(0)+"/"+x(1),1),x(0)) ).
     //将网页相同的聚合起来
      groupBy(_._2).
     //      Array((sina.com,
       //    CompactBuffer(
      //     ((sina.com/news,1),sina.com), ((sina.com/news,1),sina.com),
       //    ((sina.com/news,1),sina.com)  为 x._2
       map(x=>(x._1,x._2.map(x=>x._1).
     //  对这个((sina.com/news,1）聚合
       groupBy(_._1).
     //加操作                                                排序
       mapValues(x=>x.foldLeft(0)(_+_._2)))).map(x=>(x._1,x._2.toList.sortBy( -_._2).take(3)))
       .saveAsTextFile("E:\\mrdata\\top\\output4")

*/



line.map(_.split(" ")(1).split("/")).map(x=>(x(0),x(0)+"/"+x(1))).
    groupByKey.map(x=>(x._1,x._2.map((_,1)).groupBy(_._1).mapValues(x=>x.foldLeft(0)(_+_._2)))).map(x=>(x._1,x._2.toList.sortBy(_._2)))
  .saveAsTextFile("E:\\mrdata\\top\\output7")


  sc.stop()

}
