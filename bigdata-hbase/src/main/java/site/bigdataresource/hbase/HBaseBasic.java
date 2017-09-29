package site.bigdataresource.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.bigdataresource.utils.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.hbase.mapreduce.*;

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


    /**
     * 根据rowkey获取指定表的数据
     *
     * @param connection
     * @param tableName
     * @param rowKey
     */
    public void get(Connection connection, TableName tableName, String rowKey) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Get get = new Get(StringUtils.stringToByteArray(rowKey));//由rowkey创建一个Get
            Result result = table.get(get);
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
            Set<Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>> entries = map.entrySet();
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : entries) {
                //遍历每一行
                logger.info("ColumnFamily:{}", StringUtils.byteArrayToString(entry.getKey()));
                NavigableMap<byte[], NavigableMap<Long, byte[]>> eValue = entry.getValue();
                Set<Map.Entry<byte[], NavigableMap<Long, byte[]>>> valueMap = eValue.entrySet();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> value : valueMap) {
                    System.out.println(StringUtils.byteArrayToString(value.getKey()) + "\t");
                    NavigableMap<Long, byte[]> vs = value.getValue();
                    Set<Map.Entry<Long, byte[]>> cInfo = vs.entrySet();
                    for (Map.Entry<Long, byte[]> c : cInfo) {
                        System.out.println("Column Name=" + c.getKey() + "\t"
                                + "Column Value=" + c.getValue());
                    }
                }


            }
        } finally {
            if (table != null)
                table.close();
        }
    }

    /**
     * 获取HBase中所有表的信息
     *
     * @param connection
     * @throws IOException
     */
    public void getAllTables(Connection connection) throws IOException {
        admin = connection.getAdmin();
        //获取所有的表的描述信息
        HTableDescriptor[] hTableDescriptors = admin.listTables();


        for (HTableDescriptor tableDescriptor : hTableDescriptors) {
            //获取名称
            String name = tableDescriptor.getNameAsString();
            System.out.println("TableName=" + name);
            //显示所有的列族
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                byte[] columnFamilyName = columnFamily.getName();//列族名称
                short dfsRep = columnFamily.getDFSReplication();// 列族的备份数，默认为0


                System.out.print(StringUtils.byteArrayToString(columnFamilyName) + "\t"
                        + "DFSReplication:" + dfsRep + "\t"
                );

            }
            System.out.println("\n---------------------------------");
        }
    }

    /***
     * 添加列值
     * @param connection
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param data
     */
    public void put(Connection connection, TableName tableName,
                    String rowkey, String columnFamily, String column, String data) {
        Table table = null;
        try {
            table = connection.getTable(tableName);
            Put put = new Put(StringUtils.stringToByteArray(rowkey));
            put.addColumn(StringUtils.stringToByteArray(columnFamily),
                    StringUtils.stringToByteArray(column),
                    StringUtils.stringToByteArray(data));//添加一列，按照列族，列，列值的顺序指定
            table.put(put);//table.put(puts)可以实现批量添加
            //TODO:考虑使用Spark实现批量数据添加到HBase中
            //LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(connection.getConfiguration());


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
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
