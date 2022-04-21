package Exam

/**
  * 问题3.计算每个商品分类的成交总额，并按照从高到低排序（结果保存到MySQL中）
  */

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object B2BData3 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkIP").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
   //获取rdd
    val lines: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\exam.log")
    //切
       val pFenLeiAndCounts: RDD[(String, Float)] = lines.map(line => {
         //切分原始数据
         //   A 202.106.196.115 手机 iPhone8 8000
         //问题3.计算每个商品分类的成交总额，并按照从高到低排序（结果保存到MySQL中）
         val split: Array[String] = line.split(" ")
         //得到商品分类
         val pFenLei: String = split(2).toString
         //得到用户的消费额
         val counts: Float = split(4).toFloat
         (pFenLei, counts)
       })
    //聚合，得到每个商品分类总额
    val reduced: RDD[(String, Float)] = pFenLeiAndCounts.reduceByKey(_+_)
    //整体排序,由高到底
    val resulted: RDD[(String, Float)] = reduced.sortBy( -_._2)
      println(resulted.collect.toBuffer)

    //导入mysql
    resulted.foreachPartition(its=>{
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/haha?characterEncoding=UTF-8","root","123456")
      //如何将Id主键在mysql中传进去呢？？
      val pre: PreparedStatement = conn.prepareStatement("INSERT INTO exam3 VALUES (?,?)")
      for(it <-its){
        pre.setString(1,it._1)
        pre.setFloat(2,it._2)
        pre.executeUpdate()
      }
      if(pre != null) {
        pre.close()
      }
      if (conn != null) {
        conn.close()
      }
    })
  sc.stop()
  }
}
