package site.bigdataresource.http;

import com.google.gson.Gson;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import scala.util.parsing.json.JSONObject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by deqiangqin@gmail.com on 17-12-28.
 */
public class TBBDAPI {

    public static void main(String[] args) throws Exception {

        String[] companys = {"彩虹（江西）半导体科技有限公司","霍尔果斯德聚仁和融资租赁有限公司","宏联众装配集成房屋河北有限公司","山西欧美特汽车销售有限公司"};
        for (String company : companys)
            callWeb(company);

    }

    public static String getWebContent(String webUrl) throws IOException {
        BufferedReader in = null;
        URL url = new URL(webUrl);
        String s = null;
        try {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            InputStream is = con.getInputStream();

            InputStreamReader isr = new InputStreamReader(is);
            in = new BufferedReader(isr);
            s = in.readLine();

        } finally {
            if (in != null) {
                in.close();
            }
            return s;
        }
    }


    private static void callWeb(String company) throws Exception {

        String weburl = "http://dataapi.bbdservice.com/api/bbd_qyxx_his3/?company=" + company + "&regno=&code=&appkey=s771dzzedhza2c65rgjpiqce54wzk7hz";

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(weburl);
        //http://blog.csdn.net/h254532699/article/details/54342470
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(30000).setConnectionRequestTimeout(30000).setSocketTimeout(30000).build();

        httpGet.setConfig(requestConfig);
        CloseableHttpResponse response = httpClient.execute(httpGet);

        if (response.getStatusLine().getStatusCode() == 200) {
            System.out.println("Get Success");
            String hisRes = EntityUtils.toString(response.getEntity(), "UTF-8");
            //System.out.println(hisRes);
            //Parse hisRes
            Gson gson1 = new Gson();
            HisRes hisRes1 = gson1.fromJson(hisRes, HisRes.class);
            if (0 != hisRes1.getErr_code()) {
                System.out.println(hisRes);
                return;
            }else {
                System.out.println(hisRes);
            }


            long start = System.currentTimeMillis();
            //Thread.sleep(30000);
            int i = 0;
            for (; i < 60; i++) {

                weburl = "http://dataapi.bbdservice.com/api/bbd_qyxx/?company=" + company + "&qyxx_id=&appkey=s771dzzedhza2c65rgjpiqce54wzk7hz";
                HttpGet httpGet1 = new HttpGet(weburl);
                httpGet1.setConfig(requestConfig);
                CloseableHttpResponse response1 = httpClient.execute(httpGet1);
                if (response1.getStatusLine().getStatusCode() == 200) {
                    String res = EntityUtils.toString(response1.getEntity(), "UTF-8");//

                    Gson gson = new Gson();
                    Message message = gson.fromJson(res, Message.class);

                    Integer rsize = message.getRsize();
                    if (rsize == 1) {
                        break;
                    }

                } else {
                    System.out.println("response1 get error");
                }

                Thread.sleep(10000);
            }


            long end = System.currentTimeMillis();
            System.out.println("----------------------------\nCost Time=" + (end - start) + " ms");
            if (i == 60)
                System.out.println("Crawl write into data platform failed");

        } else {
            System.out.println("Connection Failed");
        }
    }

    class HisRes {
        private String msg;

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public Integer getErr_code() {
            return err_code;
        }

        public void setErr_code(Integer err_code) {
            this.err_code = err_code;
        }

        private Integer err_code;

    }

    class Message {
        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public Integer getRsize() {
            return rsize;
        }

        public void setRsize(Integer rsize) {
            this.rsize = rsize;
        }

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        public Integer getErr_code() {
            return err_code;
        }

        public void setErr_code(Integer err_code) {
            this.err_code = err_code;
        }

//        public String getResults() {
//            return results;
//        }
//
//        public void setResults(String results) {
//            this.results = results;
//        }

        public String getOrder_id() {
            return order_id;
        }

        public void setOrder_id(String order_id) {
            this.order_id = order_id;
        }

        public String getClient_id() {
            return client_id;
        }

        public void setClient_id(String client_id) {
            this.client_id = client_id;
        }

        private String msg;
        private Integer rsize;
        private Integer total;
        private Integer err_code;
        //private String results;
        private String order_id;
        private String client_id;
    }
}
