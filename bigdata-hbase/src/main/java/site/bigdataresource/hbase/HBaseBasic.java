package site.bigdataresource.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase的基本操作包括以下功能——
 * 1)、创建表；
 * 2)、浏览当前HBase中有哪些表
 * 3)、获取表中指定Rowkey的值；
 * 4)、往HBase的表中插入数据（单条插入/批量插入）;
 * 5)、删除表
 * Created by deqiangqin@gmail.com on 9/21/17.
 */
public class HBaseBasic {

    //使用的是org.slf4j的包
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Admin admin = null;

    public HBaseBasic() {
        System.out.println("site.bigdataresource.hbase.HBaseBasic");

    }

    public Connection initialConnection() throws Exception {
        //创建HBase的连接
        Connection connection = null;
        connection = HBaseConnectionUtils.getConnection();
        return connection;
    }


    public void get(Connection conn, TableName tableName, String rowKey) {

    }

    public void getAllTables(Connection connection) throws IOException {
//        HBaseAdmin admin=new HBaseAdmin(connection)
        admin = connection.getAdmin();
        HTableDescriptor[] hTableDescriptors = admin.listTables();


        for (HTableDescriptor tableDescriptor : hTableDescriptors) {
            String name = tableDescriptor.getNameAsString();
            System.out.println("TableName="+name);
            //显示所有的列族
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                byte[] columnFamilyName = columnFamily.getName();
                System.out.print(new String(columnFamilyName) + "\t");

            }
            System.out.println("\n---------------------------------");
        }
    }

    public void put(Connection connection, TableName tableName, String rowkey, String columnFamily, String column, String data) {
//        connection.
    }

    /**
     * 删除HBase中的表
     *
     * @param connection
     * @param tableName
     * @return 删除成功返回0；失败返回1；异常返回-1；
     * @throws IOException
     */
    public int deletedTable(Connection connection, TableName tableName) throws IOException {
        int res = -1;
        try {
            admin = connection.getAdmin();
            boolean flag = admin.tableExists(tableName);//首先,判断该表是否存在
            if (flag) {
                admin.disableTable(tableName);//其次,禁用该表
                admin.deleteTable(tableName);//最后,完全删除该表
                res = 0;//删除成功返回值0；
            } else {
                res = 1;//当表不存在的时候返回1；
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                admin.close();
            }
            return res;//出现异常的时候返回-1;
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseBasic hBaseBasic = new HBaseBasic();
        Connection con = hBaseBasic.initialConnection();
        hBaseBasic.getAllTables(con);


        //创建HBase的连接
//        Connection connection = null;
//        connection = HBaseConnectionUtils.getConnection();
//
//        //指定表名
//        TableName tableName = TableName.valueOf("bigdata");
//        String conlumnFamliy1 = "cf1";
//        String conlumnFamliy2 = "cf2";
        //createHBaseTable(connection, tableName, conlumnFamliy1, conlumnFamliy2);


    }

    /***
     * 创建HBase表
     * @param connection
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public void createHBaseTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
        //在HBase中创建表
        Admin admin = null;
        admin = connection.getAdmin();//通过连接获取管理员
        if (admin.tableExists(tableName)) {
            System.out.println("Table {" + tableName + "} exists!");

        } else {
            //定义表
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String cf : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf));//指定表的列族

            }

            admin.createTable(tableDescriptor);
            System.out.println("Create Table Success!");
            logger.info("create table:{} success!", tableName.getName());
        }

        if (admin != null) {
            admin.close();
        }
    }


}
