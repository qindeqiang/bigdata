package site.bigdataresource.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The Demo From Spark Examples
 * Created by deqiangqin@gmail.com on 9/21/17.
 * path:site/bigdataresource/spark/JavaWordCount.java
 */
public class JavaWordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {
        //tPattern();


        if (args.length < 2) {
            System.err.println("Usage:JavaWordCount <file>,<master>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("site.bigdataresource.spark.JavaWordCount");
        if (args[1].toLowerCase().equals("n")) {
            sparkConf.setMaster("local[2]");
        }

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        //RDD表示分布式存储，RDD<String>表示分布式存储的数据类型是String的
        //返回了所有的行
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        //FlatMap的功能是将结果map后Flat
        //其中，FlatMapFunction的参数：第一个表示输入值的类型，第二个表示返回值的类型；
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                //此时的line是lines中的一个
                return Arrays.asList(SPACE.split(line)).iterator();
            }
        });


        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                //此时对words中的每一个操作，就是word；
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //按照Key聚合
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //统计所有的，此时是一个action操作,spark中的操作分为action和transformation操作;
        //action是真正的触发spark提交任务
        //返回的结果将一个分布式的结果聚合到了一起，此时可以使用Java单机进行操作了
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple2 : output) {
            System.out.println(tuple2._1() + ":\t" + tuple2._2());
        }

        //另外：在spark的程序中分布式的时候不支持打印System.out.println


        spark.stop();

    }

    /**
     * TODO:Test Pattern
     * 测试正则表达式的使用
     * 正则表达式能够实现切割和匹配的功能，还有其他很多的功能；
     */
    private static void tPattern() {
        String name = "qin de qiang";
        String[] splited = SPACE.split(name);
        List<String> list = Arrays.asList(splited);

        System.out.println(list.size());
        System.out.println(list.get(2));

        Iterator<String> iterator = list.iterator();
        String s = iterator.next();
        System.out.println(s);
        iterator.remove();
    }
}
