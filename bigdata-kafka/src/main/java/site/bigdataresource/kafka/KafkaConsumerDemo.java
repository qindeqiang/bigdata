package site.bigdataresource.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by deqiangqin@gmail.com on 9/24/17.
 */
public class KafkaConsumerDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "finance-03:9092");
        props.put("group.id", "group-id-00");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();//其中key是主题的名称
        System.out.println("The topics' count=" + topics.size());

        //打印出每一个主题的partition的数量
        for (String topic : topics.keySet()) {
            List<PartitionInfo> partitionInfos = topics.get(topic);
            int partitionSizes = partitionInfos.size();
            if("dp_incre_datas".equals(topic)){
                System.err.println("-----\nTopic:" + topic + ",\tpartition numbers=" + partitionSizes);
            }
            System.out.println("-----\nTopic:" + topic + ",\tpartition numbers=" + partitionSizes);
        }
    }


}
