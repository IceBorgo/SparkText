package SortByMy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TupleOrder3 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("TupleOrder").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(Array("laoduan 30 99","laozhao 29 9999","laoyang 28 99","laoli 28 98"))

    val tupleRDD= lines.map(line => {
      val split: Array[String] = line.split(" ")
      val teacher: String = split(0)
      val age: Int = split(1).toInt
      val fv: Int = split(2).toInt
    (teacher, age, fv)
    })
    import SortRules._
    val sorted: RDD[(String, Int, Int)] = tupleRDD.sortBy(tp=>Women(tp._2,tp._3))

    println(sorted.collect.toBuffer)
  }
}
//为了实现serializeble
case class Women(age:Int,fv:Int)