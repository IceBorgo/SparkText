import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartiTopTeacher extends App{

  private val conf: SparkConf = new SparkConf().setAppName("PartiTopTeacher").setMaster("local[1]")

  private val sc: SparkContext = new SparkContext(conf)
  //获得rdd
  private val line: RDD[String] = sc.textFile("hdfs://master:9000/teacher.log")
  //             http://bigdata.edu360.cn/laozhang
 private val split: RDD[Array[String]] = line.map(_.split("/"))


}
