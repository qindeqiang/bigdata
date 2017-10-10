package site.bigdataresource.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * 创建JavaSparkSession单例模式
 */
public class JavaSparkSessionSingleton {

    private static transient SparkSession instance = null;

    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        return instance;
    }

    public static SparkSession getInstance(String appName, String isLocalMode) {
        if (instance == null) {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setAppName(appName);
            if ("y".equals(isLocalMode.toLowerCase()))
                sparkConf.setMaster("local[2]");

            SparkSession.builder().config(sparkConf).getOrCreate();
        }
        return instance;
    }
}

