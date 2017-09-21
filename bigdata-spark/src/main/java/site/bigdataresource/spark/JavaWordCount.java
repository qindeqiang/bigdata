package site.bigdataresource.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * 功能：是为了实现使用RDD统计文件中的单词数目
 * Created by bigdata on 9/9/17.
 */
public class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("必须指定参数，而且参数必须是文件的路径");
            System.exit(1); //异常了就退出
        }


//        SparkSession sparkSession=SparkSession.builder()
        SparkSession sparkSession = SparkSession.builder().appName("JavaWordCount").getOrCreate();

        JavaRDD<String> lines = sparkSession.read().textFile(args[0]).javaRDD();
        lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String s) throws Exception {
                String[] splits = SPACE.split(s);
                //TODO:数组转换为iterator是如何实现的?
                Iterator<String> res = null;
                return res;
            }
        });
    }
}
