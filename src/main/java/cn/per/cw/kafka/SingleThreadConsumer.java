package cn.per.cw.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class SingleThreadConsumer {
    private static Logger logger = LoggerFactory.getLogger(SingleThreadConsumer.class);
    private static KafkaConsumer<String, String> consumer;

    static {
        try {
            InputStream props = Resources.getResource("consumer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            consumer=new KafkaConsumer<>(properties);

        } catch (IOException e) {
            logger.error("producer initialize failed:{}", e.getMessage());
        }
    }
    public static void main(String[] args){
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
