package com.akbarseptriyan.app;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        ConsumerTest kafkaConsumer = new ConsumerTest();
        kafkaConsumer.run();

    }
}
