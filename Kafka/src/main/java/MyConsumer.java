
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;


import java.io.*;
import java.util.*;

public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.29.4.17:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID请使用学号，不同组应该使用不同的GROUP。
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "201250xxx");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2023\";");

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList("transaction"));

        // 已处理过的消息的集合
        Set<String> processedMessages = new HashSet<>();



        // 统计变量
        long totalProcessed = 0;
        long totalLatency = 0;
        long maxLatency = Long.MIN_VALUE;
        long minLatency = Long.MAX_VALUE;



        // 会从最新数据开始消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // 获取消息数据
                String message = record.value();

                String fileName1="F:\\kafkaTest\\test.txt";
                String fileName2="F:\\kafkaTest\\special.txt";


                if (processedMessages.contains(message)) {
                    System.out.println("Processed: " + message);
                    continue;
                }
                // 将消息加入已处理集合
                processedMessages.add(message);

                // 处理消息
                System.out.println(message);


                long startTime = System.currentTimeMillis();
                writeTXT(fileName1,message);
                long endTime = System.currentTimeMillis();
                long latency = endTime - startTime;

                // 获取消息头
                Header groupIdHeader = record.headers().lastHeader("groupId");
                if (Objects.nonNull(groupIdHeader)) {
                    byte[] groupId = groupIdHeader.value();
                    // 此处yourGroupId替换成你们组的组号
                    if(Arrays.equals("1".getBytes(), groupId)){
                        // 额外记录这条数据
                        System.out.println("Special: "+record.value());

                        startTime = System.currentTimeMillis();
                        writeTXT(fileName2,message);
                        endTime = System.currentTimeMillis();
                        latency = endTime - startTime;
                    }

                }
                //更新统计变量
                totalProcessed++;
                totalLatency += latency;
                if (latency > maxLatency) {
                    maxLatency = latency;
                }
                if (latency < minLatency) {
                    minLatency = latency;
                }
            }
            // 输出统计信息
            System.out.println("Total processed: " + totalProcessed);
            System.out.println("Average latency: " + (totalLatency / totalProcessed) + "ms");
            System.out.println("Max latency: " + maxLatency + "ms");
            System.out.println("Min latency: " + minLatency + "ms");

            // 睡眠一段时间，避免过于频繁地打印统计信息
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void writeTXT(String fileName,String content){
        try {
            // 防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw
            /* 写入Txt文件 */
            File file= new File(fileName);// 相对路径，如果没有则要建立一个新的output。txt文件
            BufferedWriter out = new BufferedWriter(new FileWriter(file,true));
            out.write(content); // \r\n即为换行
            out.write("\r\n");
            out.flush(); // 把缓存区内容压入文件
            out.close(); // 最后记得关闭文件
        } catch (Exception e) {
            e.printStackTrace();
        }
    }






}