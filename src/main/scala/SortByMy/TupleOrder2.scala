package SortByMy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TupleOrder2 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("TupleOrder").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(Array("laoduan 30 99","laozhao 29 9999","laoyang 28 99","laoli 28 98"))

    val userRDD: RDD[User] = lines.map(line => {
      val split: Array[String] = line.split(" ")
      val teacher: String = split(0)
      val age: Int = split(1).toInt
      val fv: Int = split(2).toInt
      new User(teacher, age, fv)
    })
    val sorted: RDD[User] = userRDD.sortBy(x=>new User(x.teacher,x.age,x.fv))


    println(sorted.collect.toBuffer)
  }
}
//case样本例实现了Serializable接口，不用写with Serializable
case class User(val teacher:String,val age: Int,val fv: Int) extends Ordered[User] {
  override def compare(that: User): Int = {
    //this当前值，that要比的值
    if(this.fv==that.fv){
      this.age-that.age
    }else{
      that.fv-this.fv
    }
  }

  override def toString = s"User($teacher, $age, $fv)"
}