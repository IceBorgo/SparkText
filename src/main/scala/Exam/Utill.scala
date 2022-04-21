package Exam

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object Utill {

   /**
    *自定义ip转换成long型
    */
def ip2Long(ip:String):Long={
  val fragments = ip.split("[.]")
  var ipNum = 0L
  for (i <- 0 until fragments.length){
    ipNum =  fragments(i).toLong | ipNum << 8L
  }
  ipNum
}

  /**
    * 二分法,针对有序数据
    */
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  /**
    * 二分法,加了个city针对有序数据
    */
  def binarySearch2(lines: Array[(Long, Long, String,String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  /**
    *将ip地域数据文件进行切，得到(start,end,province,city)的array数组
    */
  def readRules(path: String): Array[(Long, Long, String,String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      val city = fileds(7)
      (startNum, endNum, province,city)
    }).toArray
    rules
  }


  def data2Mysql(its: Iterator[(String,Float)])={
    //将数据直接传送到windows端的mysql，haha是table名，
    // 我windows端的mysql用户名为root，密码123456，linux端用户名为root，密码111111
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/haha?characterEncoding=UTF-8","root","123456")
    //如何将Id主键在mysql中传进去呢？？
    val pre: PreparedStatement = conn.prepareStatement("INSERT INTO exam1 VALUES (?,?)")
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
  }
}

