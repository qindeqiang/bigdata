package site.bigdataresource.es.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.EsSparkSQL;
import site.bigdataresource.spark.*;

/**
 * Created by deqiangqin@gmail.com on 11/5/17.
 */
public class SparkReadES {

    private static SparkSession spark;

    public static void main(String[] args) {

        if (args.length < 0) {
            System.err.println("Must set run mode");
            System.exit(1);
        }
        SparkReadES sparkReadES = new SparkReadES();
        //sparkReadES.initSparkEs();
        sparkReadES.esDF("megacorp/employee");
        spark.stop();
    }

    public SparkReadES() {
        SparkConf conf = new SparkConf();
        conf.setAppName(SparkReadES.class.getName());
        //conf.set("spark.sql.warehouse.dir", "hdfs://10.28.52.28:8020/user/hive/warehouse");
        //conf.set("hive.metastore.uris", "thrift://10.28.52.14:9083");
        conf.setMaster("local[2]");

        conf.set("es.index.auto.create", "true");
        conf.set("es.nodes", "10.28.200.209");
        conf.set("es.port", "9200");

        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    /***
     *
     */
    private void initSparkEs() {
        //00002ed9200e4ac2b7592ceb3dc1debc
        //resouce的组成：index/type
        //TODO:使用spark读取单条数据是否很快,如果直接使用http是否更快一些
        String resource = "qyxx_basic/qyxx_basic";
        Dataset<Row> ds = EsSparkSQL.esDF(spark, resource);
        ds.printSchema();
        Dataset<Row> t = ds.where("bbd_qyxx_id='00002ed9200e4ac2b7592ceb3dc1debc'");
        t.show();
    }

    private void esDF(String resource) {
        Dataset<Row> ds = EsSparkSQL.esDF(spark, resource);
        System.out.println(ds.count());
    }


    /***
     *
     * @param spark
     * @param resource Resource就是ES的index/type
     * @return
     */
    public static Dataset<Row> readES2DF(SparkSession spark, String resource) {
        EsSparkSQL.esDF(spark, resource);
        return null;
    }
}
