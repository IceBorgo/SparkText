package SparkSQLUpdate

/**
  *
  * 注意点：
buffer相当于一个中间缓存表，buffer(0),buffer(1),可以认为是中间缓存表的第一列，第二列。
由表头structType约束他的类型，Struct参数的位置是有顺序的，决定了buffer中0，1位置。
每个分区计算是，input: Row，看具体逻辑需要每一行第几列的参数（0开始计数），本次逻辑就一类，固取0.
  */

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object UdafText {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("1").master("local[2]").getOrCreate()

    import spark.implicits._

    val geomean = new GeoMean

    //range的表头就叫id
    val range = spark.range(1, 11)

    //注册函数
    //spark.udf.register("gm", geomean)
    //将range这个Dataset[Long]注册成视图
    //range.createTempView("v_range")
    //range的表头就叫id，所以传id进去
    //val result = spark.sql("SELECT gm(id) result FROM v_range")


  //不用groupby就能用agg函数
    val result = range.agg(geomean($"id") as "geomean")

    result.show()


    spark.stop()

  }
}


class GeoMean extends UserDefinedAggregateFunction {


  //输入数据的类型
  override def inputSchema: StructType = StructType(List(
    //名字无所谓，输入的数据，double最好了
    StructField("value", DoubleType)
  ))

  //产生中间结果的数据类型，bufferSchema（中间结果）
  override def bufferSchema: StructType = StructType(List(
    //上下都无所谓，不知道下面脚标需要变吗？？？是的要变，第一个为第零字段，第二个为第一个字段
    //相乘之后返回的积
    StructField("product", DoubleType),
    //参与运算数字的个数
    StructField("counts", LongType)
  ))

  //最终返回的结果类型，一般都是true
  override def dataType: DataType = DoubleType


  //确保一致性 一般用true
  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //相乘的初始值
    buffer(0) = 1.0
    //参与运算数字的个数的初始值
    buffer(1) = 0L
  }

  //每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //每有一个数字参与运算就进行相乘（包含中间结果），
    // input.getDouble(0)中的0表示输入df表中的第一列，因为该程序只有一列
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    //参与运算数据的个数也有更新
    buffer(1) = buffer.getLong(1) + 1L
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区计算的结果进行相乘
    buffer1(0) =  buffer1.getDouble(0) * buffer2.getDouble(0)
    //每个分区参与预算的中间结果进行相加
    buffer1(1) =  buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(0), 1.toDouble / buffer.getLong(1))
  }


}