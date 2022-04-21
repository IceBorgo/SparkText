package SparkSQLUpdate

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object MyUtills {
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
    *将ip地域数据文件进行切，得到(start,end,province)的array数组
    */
  def readRules(path: String): Array[(Long, Long, String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip规则进行整理，并放入到内存
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }


  def data2Mysql(its: Iterator[(String,Int)])={
    //将数据直接传送到windows端的mysql，haha是table名，
    // 我windows端的mysql用户名为root，密码123456，linux端用户名为root，密码111111
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/haha?characterEncoding=UTF-8","root","123456")
    //如何将Id主键在mysql中传进去呢？？
    val pre: PreparedStatement = conn.prepareStatement("INSERT INTO ip VALUES (?,?)")
    for(it <-its){
      pre.setString(1,it._1)
      pre.setInt(2,it._2)
      pre.executeUpdate()
    }
    if(pre != null) {
      pre.close()
    }
    if (conn != null) {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {
    //读取ip地域文件，得到ipRules的array
    val ipRules: Array[(Long, Long, String)] = readRules("E:\\java资料\\spark-1\\spark安装\\ip\\ip.txt")
    //将ip转为long
  val ipLong: Long = ip2Long("114.215.43.42")
    //将转换好的ip通过二分法查出对应的脚标
    val index: Int = binarySearch(ipRules,ipLong)
    //取出省份
    val province: String = ipRules(index)._3
    println(province)


  }




}
