package site.bigdataresource.es.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
//import org.spark_project.guava.collect.ImmutableMap;

/**
 * Created by deqiangqin@gmail.com on 11/5/17.
 * root
 * |-- bbd_qyxx_id: string (nullable = true)
 * |-- id: integer (nullable = true)
 * |-- bbd_dotime: date (nullable = true)
 * |-- bbd_type: string (nullable = true)
 * |-- bbd_uptime: integer (nullable = true)
 * |-- case_code: string (nullable = true)
 * |-- case_create_time: date (nullable = true)
 * |-- case_state: string (nullable = true)
 * |-- company_name: string (nullable = true)
 * |-- exec_court_name: string (nullable = true)
 * |-- exec_subject: float (nullable = true)
 * |-- idtype: string (nullable = true)
 * |-- pname: string (nullable = true)
 * |-- pname_id: string (nullable = true)
 * |-- bbd_xgxx_id: string (nullable = true)
 * |-- create_time: timestamp (nullable = true)
 * |-- bbd_url: string (nullable = true)
 * |-- bbd_source: string (nullable = true)
 * |-- type: string (nullable = true)
 * |-- id_type: integer (nullable = true)
 */
public class SparkSave2ES implements Serializable {

    private static SparkSession spark;

    public SparkSave2ES() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        SparkConf conf = new SparkConf();
        conf.setAppName(SparkReadES.class.getName());
        conf.setMaster("local");
        conf.set("spark.sql.warehouse.dir", "hdfs://10.28.200.210:8020/user/hive/warehouse");

        /**
         conf.set("es.index.auto.create", "true");
         conf.set("es.nodes", "10.28.200.209");
         conf.set("es.port", "9200");
         */

        spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

    }


    public static void main(String[] args) throws Exception {
        SparkSave2ES sparkES = new SparkSave2ES();
        Dataset<Row> ds = sparkES.getDs();
        ds.printSchema();
        ds.repartition(1);
        sparkES.save2ESWithTcp(ds);
        spark.stop();
    }

    private static void test1() {
        String resource = "en/en_region";

        SparkSave2ES sparkSave2ES = new SparkSave2ES();

        //Get DS
        Dataset<Row> ds = spark.read().json("file:///home/bigdata/en/src_bbd_en_region/");

        //Set _id
        Map<String, String> map = new HashMap<String, String>();
        map.put("es.mapping.id", "id");

        //Save To Es
        //TODO:
        JavaEsSparkSQL.saveToEs(ds, resource, map);
    }

    private static void test2() {
        String resource = "en/en_region1";

        SparkSave2ES sparkSave2ES = new SparkSave2ES();

        //Get DS
        Dataset<Row> ds = spark.read().json("file:///home/bigdata/en/src_bbd_en_region/");

        //Set _id
        Map<String, String> map = new HashMap<String, String>();
        //该map的值必须来自DS，如何组装出新的？
        map.put("es.mapping.id", "id");
        map.put("es.mapping.exclude", "id");

        //Save To Es
        //TODO:
        JavaEsSparkSQL.saveToEs(ds, resource, map);
    }

    public void save2ESWithTcp(Dataset<Row> rows) {
        rows.foreachPartition(new ForeachPartitionFunction<Row>() {
            public void call(Iterator<Row> iterator) throws Exception {
                TransportClient client = initEsConnection();
                BulkRequestBuilder bulkRequest = client.prepareBulk();

                //client.prepareIndex()

                while (iterator.hasNext()) {
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex("finance_ip", "src_en_region_4");

                    HashMap<String, String> source = new HashMap<String, String>();
                    Row row = iterator.next();

                    String id = row.get(1).toString();
                    String area = row.get(0).toString();

                    source.put("area", area);
                    source.put("id", id);

                    indexRequestBuilder.setSource(source);
                    bulkRequest.add(indexRequestBuilder);

                }


                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
                    if (bulkItemResponses.hasFailures()) {
                        //TODO:Add To Log
                        System.out.println("Process Bulk Request Failed,Cause By:" + bulkItemResponses.buildFailureMessage());
                    }
                }

                //client.close();

            }
        });


    }

    private Dataset<Row> getDs() {
        String hdfsPath = "hdfs://10.28.200.210:8020/user/zhixing";
        String localPath = "file:///home/bigdata/en/src_bbd_en_region/";
        Dataset<Row> ds = spark.read().json(localPath);
        return ds;
    }

    private TransportClient initEsConnection() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "data-test")
                .put("client.transport.sniff", false).build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.28.100.31"), 59303));
        return client;
    }


}
