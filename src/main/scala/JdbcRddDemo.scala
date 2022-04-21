import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //创建RDD，这个RDD会记录以后从MySQL中读数据
    //new 了RDD，里面没有真正要计算的数据，而是告诉这个RDD，以后触发Action时到哪里读取数据
    val jdbcRDD = new JdbcRDD(
      sc,
      ()=>{
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/taobao?characterEncoding=UTF-8","root","123456")
        conn
      },
      "select * from items where items_id >=? and items_id<=?",
      1,
      5,
      2,
      rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val price = rs.getString(3)
        (id, name, price)
      }
    )
    val collected: Array[(Int, String, String)] = jdbcRDD.collect()
    println(collected.toBuffer)
  }
}
