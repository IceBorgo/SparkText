package Test

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ 2021/10/17 Liffett 11:01
  *
  */


/**
  *
  */

object test {

  def main(args: Array[String]): Unit = {

    //1、创建sparkConf
    val conf: SparkConf = new SparkConf().setAppName("mySpark").setMaster("local[*]")
    //2、创建sparkContext
    val sc = new SparkContext(conf)
    //3、读数据
    val lines: RDD[String] = sc.textFile("src/data/data.txt")
    val keyAndOne = lines.flatMap(_.split(" ")).map((_, 1))


    }



}
