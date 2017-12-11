package site.bigdataresource.es.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * Created by deqiangqin@gmail.com on 11/10/17.
 */
public class WriteESWithHTTP {

    private static SparkSession spark;
    private final static String ES_INDEX = "finance_ip";
    private final static String ES_INDEX_TYPE = "zhixing2";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("");
            System.exit(1);
        }


        int numpartition = Integer.valueOf(args[0]);

        SparkConf conf = new SparkConf();
        conf.setAppName("site.bigdataresource.es.spark.example.WriteESWithHTTP");
        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes", "10.28.100.30");
        conf.set("es.port", "39202");
        conf.set("spark.sql.warehouse.dir", "hdfs://10.28.200.210:8020/user/hive/warehouse");

        spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
        String appId = spark.sparkContext().applicationId();

        String hdfsPath = "hdfs://10.28.200.210:8020/user/zhixing";
        Dataset<Row> ds = spark.read().orc(hdfsPath);

        ds.repartition(numpartition);
        JavaEsSparkSQL.saveToEs(ds, ES_INDEX + "/" + ES_INDEX_TYPE);

        spark.stop();

    }
}
