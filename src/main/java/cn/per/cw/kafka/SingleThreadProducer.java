package cn.per.cw.kafka;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class SingleThreadProducer {
    private static Logger logger = LoggerFactory.getLogger(SingleThreadProducer.class);
    private static KafkaProducer<String, String> producer;

    static {
        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        } catch (IOException e) {
            logger.error("producer initialize failed:{}", e.getMessage());
        }
    }



    public static void executeNoBlocking(final String topicName) {
        IntStream.range(0, 10).forEach(i -> {
            try {
                producer.send(new ProducerRecord<>(
                        topicName,
                        String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i))).get();
                logger.info("send {} message ", i);
            } catch (InterruptedException e) {
                logger.error("interrupt error: {}", e.getMessage());
            } catch (ExecutionException e) {
                logger.error("execute error: {}", e.getMessage());
            }
        });
    }

    public static void executeBlocking(final String topicName) {
        IntStream.range(0, 10).forEach(i -> {
            try {
                Future<RecordMetadata> record = producer.send(new ProducerRecord<>(
                                topicName,
                                String.format("{\"type\":\"aaaaaaa\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)),
                        (metadata, e) -> System.out.println("The offset of the record we just sent is: " + metadata.partition() + "|" + metadata.offset())
                );
                record.get();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }

        });
    }

    /**
     * 事务producer
     * @param topicName topic名称
     */
    public static void transactionEexecute(final String topicName){
        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i)));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }

    public static void main(String[] args) {
        executeBlocking("my-topic");

        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
