package site.bigdataresource.utils;

import java.io.*;
import java.util.Properties;

/**
 * 配置文件工具集
 * Created by deqiangqin@gmail.com on 9/21/17.
 */
public class PropertiesUtils {


    public static Properties load(File file) {
        InputStream in = null;

        try {
            in = new FileInputStream(file);
            Properties props = new Properties();
            props.load(in);

            return props;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Closeable closeable = (Closeable) in;
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return null;
    }

    public static Properties load(String path) throws IOException {
        InputStream in = null;
        in = ClassUtils.getClassLoader().getResourceAsStream(path);
        Properties props = new Properties();
        props.load(in);
        return props;
    }

    //TODO:该方法将要被删除，移植到Test目录下面
    public static void main(String[] args) {
        Properties props = null;
        try {
            props = PropertiesUtils.load("hbase.properties");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage().toString());
        }
        String clientPort = props.getProperty("hbase.zookeeper.property.clientPort");
        System.out.println(clientPort);
    }
}
