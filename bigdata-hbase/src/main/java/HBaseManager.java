import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by bigdata on 9/8/17.
 */
public class HBaseManager extends Thread {



    public HBaseManager(){
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master","10.28.200.210:60000");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.zookeeper.quorum","10.28.100.56,10.28.100.57,10.28.100.58");
        try {
            HTable table = new HTable(config, Bytes.toBytes("demo_table"));
            HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
