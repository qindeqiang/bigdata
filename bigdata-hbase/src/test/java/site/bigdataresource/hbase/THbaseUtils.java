package site.bigdataresource.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by deqiangqin@gmail.com on 9/24/17.
 */
public class THbaseUtils {


    private HBaseBasic hBaseBasic;
    private Connection connection;

    @Before
    public void setup() throws Exception {
        hBaseBasic = new HBaseBasic();
        connection = hBaseBasic.initialConnection();
    }

    @Test
    public void testCreateHbaseTable() throws IOException {
        TableName tableName = TableName.valueOf("daqin_demo");
        String cf1 = "a";
        String cf2 = "b";
        hBaseBasic.createHBaseTable(connection, tableName, cf1, cf2);
    }

    @Test
    public void testGetAllTables() throws IOException {
        System.out.println("TestGetAllTables");

        hBaseBasic.getAllTables(connection);
    }
}
