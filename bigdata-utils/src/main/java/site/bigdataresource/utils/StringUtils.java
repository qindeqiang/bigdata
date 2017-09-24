package site.bigdataresource.utils;

/**
 * Created by deqiangqin@gmail.com on 9/24/17.
 */
public class StringUtils {

    /**
     * 将byte数组转换为字符串
     *
     * @param byteArray
     * @return
     */
    public static String byteArrayToString(byte[] byteArray) {
        if (byteArray == null) {
            return null;
        }
        String str = new String(byteArray);
        return str;
    }

    /**
     * 将字符串转换为Byte数组
     * 此方法常用在网络传输的时候
     * 参考文章:http://www.jianshu.com/p/17e771cb34aa
     *
     * @param str
     * @return
     */
    public static byte[] stringToByteArray(String str) {
        if (str == null) {
            return null;
        }

        byte[] bytes = str.getBytes();
        return bytes;
    }
}
