package Exam

import java.sql.{Connection, DriverManager}
import java.util



import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.glassfish.jersey.client.HttpUrlConnectorProvider.ConnectionFactory
import org.spark_project.guava.primitives.Bytes
import org.xml.sax.HandlerBase

import scala.collection.mutable



/**
  * 问题4.构建每一个用户的用户画像，就是根据用户购买的具体商品，给用户打上一个标签，为将来的商品推荐系统作数据支撑
  */
object B2BData4 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkIP").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    /**
      * first,重数据库拿到用户画像，并装在array中,将arry的数据放入hashmap中
      */
    //依据画像在mysql中建立数据库
    //从数据库获取标签
    //创建jdbcRDD
    val userFaceRdd: JdbcRDD[( String, String)] = new JdbcRDD(
      sc,
      //获取getconnection
      () => {
        val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/haha?characterEncoding=UTF-8", "root", "123456")
        conn
      },
      "select * from userFace where id >=? and id<=?",
      1,
      16,
      2,
      rs => {
        //val id = rs.getInt(1)
        val product = rs.getString(2)
        val biaoqian = rs.getString(3)
        ( product, biaoqian)
      }
    )

    //获取用户画像的array
    val userFaceArry = userFaceRdd.collect()
    //用一个hashmap装(k,Array)
    var userFaceMap = new mutable.HashMap[String,util.ArrayList[String]]()

    //循环遍历将userFaceArry，装进userFaceMap中
    for(arr <- userFaceArry){
      //若存在key
      if(userFaceMap.contains(arr._1)){
        //取出values，ArrayList进行add添加新的元素
        val values: util.ArrayList[String] = userFaceMap(arr._1)
        values.add(arr._2)
        //存入hashmap
        userFaceMap.put(arr._1,values)
      }else{
        //不存在key，新建一个list，将arr._2添加进去
        var values: util.ArrayList[String] = new  util.ArrayList[String]()
        values.add(arr._2)
        //存入hashmap
        userFaceMap.put(arr._1,values)
      }
    }
    //广播到executor中
    val broadcastRef = sc.broadcast(userFaceMap)
    //  println(userFaceMap.toBuffer)
    /**
      * second,切用户数据，给用户添加用户画像
      */
    //获取rdd
    val lines: RDD[String] = sc.textFile("E:\\mrdata\\exam\\input\\exam.log")
    //切完后的到(user,product),
    val userAndFaceArr = lines.map(line => {
      //切分原始数据
      //   A 202.106.196.115 手机 iPhone8 8000
      val split: Array[String] = line.split(" ")
      //得到用户
      val user: String = split(0)
      //得到用户购买的商品
      val product: String = split(3)
      //(user,product )
      //获取游离的userFace
      val userFace: mutable.HashMap[String, util.ArrayList[String]] = broadcastRef.value
      //将通过hashmap(),获取用户画像标签,如果有该产品的画像，执行
      if(userFaceMap.contains(product)) {
        val faceArray: util.ArrayList[String] = userFace(product)
        (user, faceArray)
      }
    })
    println(userAndFaceArr.collect().toList)
    /**
      * 第三步，将用户画像存在hbase中,导入hbase maven的依赖，待续
      */

    userAndFaceArr.foreachPartition(x=>{

    /*  val hbaseConf = HBaseConfiguration.create()
      //设置zookeeper的集群
      hbaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2")
      //设置zookeeper连接端口，默认2181
      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
      hbaseConf.set("hbase.defaults.for.version.skip", "true")*/





    })

  }


}


