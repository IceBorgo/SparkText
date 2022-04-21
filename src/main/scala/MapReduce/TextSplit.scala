package MapReduce

object TextSplit extends App{

  private val line: String = "http://bigdata.edu360.cn/laozhang"

  private val split1: Array[String] = line.split("/")
  private val teacher: String = split1(3)
  private val subject: String = split1(2).split("[.]")(0)
  println(teacher+"  "+subject)

}
