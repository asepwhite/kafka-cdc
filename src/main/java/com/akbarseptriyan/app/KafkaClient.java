package com.akbarseptriyan.app;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.source.SourceTask;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class KafkaClient {

    private Properties clientProperties;
    private KafkaConsumer consumer;

    public KafkaClient(Properties clientProperties)
    {
        this.clientProperties = clientProperties;
        consumer = new KafkaConsumer<String, String>(clientProperties);
    }

    private void setProperties(Properties clientProperties){
        this.clientProperties = clientProperties;
    }

    private KafkaConsumer getConsumer(){
        return consumer;
    }

    public void printMessages(String topic){
        consumer.subscribe(Arrays.asList(topic));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }

    public void parseMessage(String topic){
        JSONParser parser = new JSONParser();
        consumer.subscribe(Arrays.asList(topic));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                try {
                    Object obj = parser.parse(record.value());
                    Object payloadObject = ((JSONObject) obj).get("payload");
                    JSONObject jsonPayloadObject = (JSONObject) payloadObject;
                    JSONObject jsonObject = (JSONObject) obj;
                    String operation = (String) jsonPayloadObject.get("op");
                    System.out.println(operation);
//                    System.out.println(obj);
                } catch (ParseException e) {
                    System.out.println("Wrong format! ");
                    e.printStackTrace();
                }
                System.out.println();
            }
        }
    }


}
