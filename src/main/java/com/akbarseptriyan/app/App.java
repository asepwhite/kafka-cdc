package com.akbarseptriyan.app;

//import org.apache.kafka.clients.consumer.Consumer;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaClient client = new KafkaClient(props);
        client.parseMessage("test.public.user");
    }
}
