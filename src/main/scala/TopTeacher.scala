import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopTeacher {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TopTeacher").setMaster("local[1]")
    val sc = new SparkContext(conf)

   /* 1.在所有的老师中求出最受欢迎的老师Top3
    sc.textFile("hdfs://master:9000/teacher.log").map((_,1)).reduceByKey(_+_).sortBy( -_._2).collect
    2.求每个学科中最受欢迎老师的top3（至少用2到三种方式实现）*/

    val line: RDD[String] = sc.textFile("E:\\mrdata\\teacher\\input\\teacher.log")
    //将每个url和一结合
    val lineAndOne: RDD[(String, Int)] = line.map((_,1))
    //相同key相同聚合，value相加
    val reduced: RDD[(String, Int)] = lineAndOne.reduceByKey(_+_)

    reduced.sortBy(_._2,false)saveAsTextFile("E:\\mrdata\\teacher\\output")

    sc.stop()





    }

}
