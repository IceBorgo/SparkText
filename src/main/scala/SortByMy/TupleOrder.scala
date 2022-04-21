package SortByMy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TupleOrder {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("TupleOrder").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(Array("laoduan 30 99","laozhao 29 9999","laoyang 28 99","laoli 28 98"))

    val tupleRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val split: Array[String] = line.split(" ")
      val teacher: String = split(0)
      val age: Int = split(1).toInt
      val fv: Int = split(2).toInt
      (teacher, age, fv)
    })
    //利用元祖内部的排序机制排序
    val sorted = tupleRDD.sortBy(tp=>(-tp._3,tp._2))
    println(sorted.collect.toBuffer)
  }
}
