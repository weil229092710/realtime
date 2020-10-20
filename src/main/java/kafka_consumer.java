import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by root on 2019/7/5 0005.
 */
public class kafka_consumer {
    public static String topic = "mongo-kafka";//"nginx";
    public static String brokerList = "192.168.5.85:9092,192.168.5.86:9092,192.168.5.87:9092";
    public static void main(String[] args) {
        kafka_consumer();
    }

    public static void kafka_consumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", "test-consumer-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("message.timeout.ms", 3000);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);



        // 不指定消费区，负载均衡，各区轮询
        consumer.subscribe(Arrays.asList(topic));
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(3000);
            for(ConsumerRecord<String, String> record : records) {
                // 这里的数据一般用多线程处理，即一个线程专门用来拉取数据，其它线程来消费数据，提高数据处理效率
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());
            }
            // "enable.auto.commit", 这个参数置为true，则自动提交offset，否则需要下面代码手动提交，这样做的好处是：假设在自动提交的间隔内发生故障（比如整个JVM进程死掉），那么有一部分消息是会被 重复消费的
            // consumer.commitSync();

            System.out.println(1);
        }
    }
}
