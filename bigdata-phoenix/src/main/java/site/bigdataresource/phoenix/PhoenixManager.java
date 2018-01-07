package site.bigdataresource.phoenix;

import java.sql.*;

/**
 * 测试成功
 * Created by deqiangqin@gmail.com on 12/11/17.
 */
public class PhoenixManager {

    public static void main(String[] args) {

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException ex) {
            System.out.println("Exception Loading Driver");
            ex.printStackTrace();
        }

        //URL的设置方法：jdbc:phoenix:192.168.122.1:2181:/hbase 最后一个是Hbase在ZK的路径
        try {
            Connection con = DriverManager.getConnection("jdbc:phoenix:192.168.122.1:2181:/hbase");
            //Statement statement = con.createStatement();

            PreparedStatement statement = con.prepareStatement("select * from WEB_STAT");
            ResultSet rset = statement.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("DOMAIN"));
            }

            statement.close();
            con.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
/**
 * 执行结果：
 * Apple.com
 Apple.com
 Google.com
 Google.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Apple.com
 Apple.com
 Apple.com
 Apple.com
 Apple.com
 Apple.com
 Apple.com
 Google.com
 Google.com
 Google.com
 Google.com
 Google.com
 Google.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 Salesforce.com
 */
