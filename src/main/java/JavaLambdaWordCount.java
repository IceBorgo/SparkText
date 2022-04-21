import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLambdaWordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("JavaLambdaWordCount").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        //jsc.textFile(args[0]).flatMap(x->Arrays.asList(x.split(" ")).iterator()).mapToPair(x->new Tuple2<>(x,1)).reduceByKey((x,y)->x+y).mapToPair(x->x.swap()).sortByKey(false).mapToPair(x->x.swap()).saveAsTextFile(args[1]);




       //指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和一组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        //调整顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        //将结果保存到hdfs
        result.saveAsTextFile(args[1]);



        jsc.close();
    }
}
