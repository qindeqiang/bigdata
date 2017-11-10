package site.bigdataresource.hbase;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import site.bigdataresource.spark.JavaSparkSessionSingleton;
import site.bigdataresource.utils.PropertiesUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**site.bigdataresource.hbase.Hdfs2HBase
 * Created by deqiangqin@gmail.com on 10/10/17.
 */
public class Hdfs2HBase {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Must Set Hbase Table");
            System.exit(1);
        }
        String hbaseTableName = args[0];
        String columnFamily = "A";
        final byte[] cf = Bytes.toBytes(columnFamily);


        Configuration hbaseConf = HBaseConfiguration.create();
        //通过读取本地配置文件的方式读取基本配置
        //TODO:配置文件的读取单独提炼出来
        Properties props = PropertiesUtils.load("hbase.properties");
        //zk的端口
        hbaseConf.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort", "2181"));
        //zk集群的地址
        hbaseConf.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));
        hbaseConf.set("hbase.defaults.for.version.skip", "true");
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName);

        Connection connection = ConnectionFactory.createConnection();
        final Table table = connection.getTable(TableName.valueOf(hbaseTableName));

        Job job = Job.getInstance(hbaseConf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Result.class);


//        JavaSparkSessionSingleton
        SparkSession spark = JavaSparkSessionSingleton.getInstance("site.bigdataresource.hbase.Hdfs2HBase", "n");

        String path = "hdfs://10.28.52.43:8020/user/hive/warehouse/dw.db/qyxx_gdxx/dt=20171009";
        Dataset<Row> ds = spark.read().orc(path);

        JavaRDD<Row> rowJavaRDD = ds.toJavaRDD();

        JavaRDD<Put> putJavaRDD = rowJavaRDD.map(new Function<Row, Put>() {
            @Override
            public Put call(Row row) throws Exception {
                Put put = new Put(Bytes.toBytes(row.getString(3) + row.getString(12) + row.get(11)));
                put.addColumn(cf, Bytes.toBytes("id"), Bytes.toBytes(row.getString(0)));
                put.addColumn(cf, Bytes.toBytes("idno"), Bytes.toBytes(row.getString(1)));
                put.addColumn(cf, Bytes.toBytes("idtype"), Bytes.toBytes(row.getString(2)));
                put.addColumn(cf, Bytes.toBytes("bbd_qyxx_id"), Bytes.toBytes(row.getString(3)));
                put.addColumn(cf, Bytes.toBytes("invest_amount"), Bytes.toBytes(row.getString(4)));
                put.addColumn(cf, Bytes.toBytes("invest_name"), Bytes.toBytes(row.getString(5)));
                put.addColumn(cf, Bytes.toBytes("invest_ratio"), Bytes.toBytes(row.getString(6)));
                put.addColumn(cf, Bytes.toBytes("name_compid"), Bytes.toBytes(row.getString(7)));
                put.addColumn(cf, Bytes.toBytes("no"), Bytes.toBytes(row.getString(8)));
                put.addColumn(cf, Bytes.toBytes("paid_contribution"), Bytes.toBytes(row.getString(9)));
                put.addColumn(cf, Bytes.toBytes("shareholder_detail"), Bytes.toBytes(row.getString(10)));
                put.addColumn(cf, Bytes.toBytes("shareholder_id"), Bytes.toBytes(row.getString(11)));
                put.addColumn(cf, Bytes.toBytes("shareholder_name"), Bytes.toBytes(row.getString(12)));
                put.addColumn(cf, Bytes.toBytes("shareholder_type"), Bytes.toBytes(row.getString(13)));
                put.addColumn(cf, Bytes.toBytes("subscribed_capital"), Bytes.toBytes(row.getString(14)));
                put.addColumn(cf, Bytes.toBytes("bbd_dotime"), Bytes.toBytes(row.getString(15)));
                put.addColumn(cf, Bytes.toBytes("company_name"), Bytes.toBytes(row.getString(16)));
                put.addColumn(cf, Bytes.toBytes("bbd_uptime"), Bytes.toBytes(row.getString(17)));
                put.addColumn(cf, Bytes.toBytes("create_time"), Bytes.toBytes(row.getString(18)));
                put.addColumn(cf, Bytes.toBytes("sumconam"), Bytes.toBytes(row.getString(19)));

                return put;
            }
        });

        putJavaRDD.foreachPartition(new VoidFunction<Iterator<Put>>() {
            @Override
            public void call(Iterator<Put> iterator) throws Exception {

                List puts = IteratorUtils.toList(iterator);
                table.put(puts);
            }
        });
    }
}
