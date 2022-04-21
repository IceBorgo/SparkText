package MapReduce


import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object others extends App{

  private val conf: SparkConf = new SparkConf().setAppName("WordCound").setMaster("local[1]")

  private val sc: SparkContext = new SparkContext(conf)

  val rdd4 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  val rdd5 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
  val rdd6 = rdd5.zip(rdd4)
  //   ArrayBuffer((1,dog), (1,cat), (2,gnu), (2,salmon), (2,rabbit), (1,turkey), (2,wolf), (2,bear), (2,bee))

  val rdd7= rdd6.combineByKey(x=>ListBuffer(x),(m:ListBuffer[String],n:String)=> m+=n,(x:ListBuffer[String],y:ListBuffer[String])=>x++=y)
  println(rdd7.collect().toBuffer)

}
