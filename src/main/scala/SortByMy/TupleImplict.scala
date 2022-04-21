package SortByMy

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TupleImplict {

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
   // val sorted: RDD[(String, Int, Int)] = tupleRDD.sortBy(boy=>new Boy(boy._1,boy._2,boy._3))


  //  println(sorted.collect.toBuffer)
  }
}

case class Boy(val teacher:String,val age: Int,val fv: Int) extends Ordered[Boy] {
  override def compare(that: Boy): Int = {
    if(this.fv==that.fv){
      this.age-that.age
    }else{
      that.fv-this.fv
    }
  }

  override def toString = s"Boy($teacher, $age, $fv)"
}
