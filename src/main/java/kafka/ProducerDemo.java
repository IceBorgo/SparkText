package kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;


public class ProducerDemo {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        for (int i = 1; i <= 100; i++)
            producer.send(new KeyedMessage<String, String>("xiaoniu","fuck" +i));
    }



}
