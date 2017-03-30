/**
 * Created by VGazzola on 15/12/16.
 */


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {


        Properties props = new Properties();
        props.put("bootstrap.servers", "10.19.7.223:9093");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("security.protocol","SSL");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("ssl.truststore.location","/Users/VGazzola/tests/kafka_2.11-0.10.0.0/kafka.client.truststore.jks");
        props.put("ssl.truststore.password","test1234");
//        props.put("ssl.keystore.location","/Users/VGazzola/tests/kafka_2.11-0.10.0.0/kafka.client.keystore.jks");
//        props.put("ssl.keystore.password","test1234");
//        props.put("ssl.key.password","test1234");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        producer.send(new ProducerRecord<String, String>("vga", "Key: from java", "Value: from java too"));

        producer.close();

    }
}
