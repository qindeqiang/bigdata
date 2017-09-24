package site.bigdataresource.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Properties;
//import site.bigdataresource.
import site.bigdataresource.utils.PropertiesUtils;

/**
 * HBase连接工具,主要是返回一个HBase的连接
 * Created by deqiangqin@gmail.com on 9/21/17.
 */
public class HBaseConnectionUtils {

    public static Connection getConnection() throws Exception {
        Configuration configuration = getHBaseConfig();
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    private static Configuration getHBaseConfig() throws IOException {
        Configuration config = HBaseConfiguration.create();
        //通过读取本地配置文件的方式读取基本配置
        //TODO:配置文件的读取单独提炼出来
        Properties props = PropertiesUtils.load("hbase.properties");
        //zk的端口
        config.set("hbase.zookeeper.property.clientPort", props.getProperty("hbase.zookeeper.property.clientPort", "2181"));
        //zk集群的地址
        config.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"));

        return config;

        /**
         * 创建配置的另外一种方法，该方法的好处是把所有的资源都加载进去了
         * config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
         config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
         */

    }
}
