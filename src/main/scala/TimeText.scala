import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat





object TimeText {

  def main(args: Array[String]): Unit = {
    import java.text.SimpleDateFormat
    import java.util.Locale
    //   18/Sep/2013:06:49:18//   18/Sep/2013:06:49:18

    /**
      * 毫秒Long转时间
      */
    val date = new Date
    date.setTime(1379458163000L)
    //加Locale.ENGLISH，就能将sep识别了
    val sdf1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    System.out.println(sdf1.format(date))

    /**
      * 时间转毫秒Long
      */
    val source = "22/Sep/2017:21:32:48"
    val time = sdf1.parse(source).getTime

    System.out.println(time)

    /**
      * "2017年3月21日,星期五,11:23:21" 注意逗号和format里"yyyy年MM月dd日,E,HH:mm:ss"的逗号保持一致性
      */
    val sdf2 = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    val time2 = sdf2.parse("2017年3月21日,星期五,11:23:21").getTime
    System.out.println(time2)

    date.setTime(1490066601000L)
    val format = sdf2.format(date)
    System.out.println(format)

    val sdf3 = FastDateFormat.getInstance(("yyyy年MM月dd日,E,HH:mm:ss"))
    val time3 = sdf3.parse("2017年3月21日,星期五,11:23:21").getTime
    System.out.println(time3)


  }
}
