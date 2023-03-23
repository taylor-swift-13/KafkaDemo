import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {
            Properties props = new Properties();
            //kafka 集群，broker-list
            props.put("bootstrap.servers", "121.43.165.220:9092");
            props.put("acks", "all");
            //重试次数
            props.put("retries", 1);
            //批次大小
            props.put("batch.size", 16384);
            //等待时间
            props.put("linger.ms", 1);
            //RecordAccumulator 缓冲区大小
            props.put("buffer.memory", 33554432);
            //实现幂等性
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = new KafkaProducer<>(props);
            try {
                // 生产单条数据示例
                String fileName="F:\\kafkaTest\\special.txt";
                BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
                String value=null;
                while((value=bufferedReader.readLine())!=""){

                    ProducerRecord<String, String> record = new ProducerRecord<>("transaction", value);
                    producer.send(record);
                    System.out.println(value);

                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
        }
    }