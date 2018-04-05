package com.akbarseptriyan.app;

import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class ProducerTest {

    private static String brokerServer= "localhost:9092";
    private static String topic = "test-topic";

    public static void main (String args[]) {

       Properties props = new Properties();
       props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServer);
       props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
       props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

       Producer<String, String> prod = new KafkaProducer<String, String>(props);

        for (int i = 70; i < 76; i++) {
            String message = "test"+i;
            prod.send(new ProducerRecord<String, String>("test-topic", message));
        }
        prod.close();
    }

}