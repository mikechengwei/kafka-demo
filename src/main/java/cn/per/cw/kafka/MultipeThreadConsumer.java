package cn.per.cw.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class MultipeThreadConsumer {

    private static Logger logger = LoggerFactory.getLogger(SingleThreadConsumer.class);

    private static ExecutorService executorService= Executors.newFixedThreadPool(10);

    private static Properties properties;
    static {
        try {
            InputStream props = Resources.getResource("consumer.props").openStream();
            properties = new Properties();
            properties.load(props);

        } catch (IOException e) {
            logger.error("producer initialize failed:{}", e.getMessage());
        }
    }

    public static  void main(String[] args){
        executorService.submit(new KafkaConsumerRunner(properties));

    }


}

class KafkaConsumerRunner implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final KafkaConsumer consumer;

    KafkaConsumerRunner(Properties properties){
        consumer=new KafkaConsumer(properties);
    }
    public void run() {
        try {
            consumer.subscribe(Arrays.asList("topic"));
            while (!closed.get()) {
                ConsumerRecords records = consumer.poll(10000);
                // Handle new records
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
