package site.bigdataresource.es.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;

/**
 * 将qyxx_basic的数据保存到
 * Created by deqiangqin@gmail.com on 11/10/17.
 */
public class WriteESWithTCP implements Serializable {

    private static SparkSession spark;
    private final static String ES_CLUSTER_NAME = "data-test";
    private final static String ES_INDEX = "finance_ip";
    private final static String ES_INDEX_TYPE = "qyxx_basic_map";
    private final static String ES_TCP_HOST = "10.28.100.31";
    private final static Integer ES_TCP_POST = 59303;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("");
            System.exit(1);
        }
        int numpartition = Integer.valueOf(args[0]);

        SparkConf conf = new SparkConf();
        conf.setAppName(WriteESWithTCP.class.getName());
        //conf.setMaster("local[2]");
        conf.set("spark.sql.warehouse.dir", "hdfs://10.28.200.210:8020/user/hive/warehouse");

        spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        String hdfsPath = "hdfs://10.28.200.210:8020/user/qyxx_basic";
        Dataset<Row> ds = spark.read().orc(hdfsPath);

        ds.repartition(numpartition);

        save2ESWithTcp(ds);

        spark.stop();
    }


    private static void save2ESWithTcp(Dataset<Row> rows) {
        rows.foreachPartition(new ForeachPartitionFunction<Row>() {
            public void call(Iterator<Row> iterator) throws Exception {
                System.setProperty("es.set.netty.runtime.available.processors", "false");
                TransportClient client = initEsConnection();
                BulkRequestBuilder bulkRequest = client.prepareBulk();

                while (iterator.hasNext()) {
                    IndexRequestBuilder indexRequestBuilder = client.prepareIndex(ES_INDEX, ES_INDEX_TYPE);

                    HashMap<String, String> source = new HashMap<String, String>();
                    Row row = iterator.next();

                    String[] fieldNames = row.schema().fieldNames();
                    for (int i = 0; i < fieldNames.length; i++) {
                        String fieldName = fieldNames[i];
                        source.put(fieldName, getRowValueByIndex(row, i));
                    }


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

    private static String getRowValueByIndex(Row row, int index) {
        try {
            return String.valueOf(row.get(index));
        } catch (Exception e) {
            return "";
        }

    }


    private static TransportClient initEsConnection() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", ES_CLUSTER_NAME)
                .put("client.transport.sniff", false).build();

        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ES_TCP_HOST), ES_TCP_POST));
        return client;
    }
}
