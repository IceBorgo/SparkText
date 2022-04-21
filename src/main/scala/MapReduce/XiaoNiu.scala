package MapReduce

import MapReduce.TextSplit.line
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object XiaoNiu  extends  App{

  private val conf: SparkConf = new SparkConf().setAppName("WordCound").setMaster("local[1]")

  private val sc: SparkContext = new SparkContext(conf)
  private val line: RDD[String] = sc.textFile("hdfs://master:9000/teacher.log")
  //全局聚合
  //line.map(_.split("/")).map(x=>(x(0)+"//"+x(2),x(3))).groupByKey.map(x=>(x._1,x._2.map((_,1))))


  /*//聚不聚和
  line.map(_.split("/")).map(x=>(x(0)+"//"+x(2),x(3))).
    map(x=>(x._1,x._2.map((_,1)).groupBy(_._1).mapValues(x=>x.foldLeft(0)(_+_._2)).toList.sortBy( -_._2)))
    .saveAsTextFile("E:\\mrdata\\output3")
*/


 // private val line1: RDD[(String, Double)] = sc.parallelize(Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0)))

    //private val result = line1.combineByKey(x=>StringBuffer[Double], (x:StringBuffer[Double], y:Double)=>x+=y, (x:StringBuffer[Double],y:StringBuffer[Double)=>x++=y]





  //重写局部聚合
//dui每行的数据进行切分,最后得到 （（学科，老师），1）
   val  subjectAndTeacherAndOne= line.map(line=>{
    val split1: Array[String] = line.split("/")
    val teacher: String = split1(3)
    val subject: String = split1(2).split("[.]")(0)
  ((subject,teacher),1)
  })
  //在全局中对(学科，老师)进行聚合
   private val reduced: RDD[((String, String), Int)] = subjectAndTeacherAndOne.reduceByKey(_+_)
  //局部聚合，在(学科，老师)小组里对学科聚合
 private val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
  //排序取前三
 private val result = grouped.mapValues(_.toList.sortBy( -_._2)).collect()

  //private val values = subjectAndTeacherAndOne.reduceByKey(_+_).groupBy(_._1._1).mapValues(_.toList.sortBy( -_._2)).collect()



  println(result.toBuffer)
  sc.stop()

}
