package com.akbarseptriyan.app;
import java.sql.*;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.source.SourceTask;
import org.json.*;

public class DBMirror {

    private static final String JBDC_DRIVER = "org.postgresql.Driver";
    private static final String DB_HOST = "jdbc:postgresql://localhost:5432/debezdb";
    private static final String DB_USER = "debezium";
    private static final String DB_PASSWORD = "dbz12345";

    private static Connection conn;
    private static Statement statement;


    public static void main (String[] args) throws SQLException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        String topic = "local.public.animals";
        consumer.subscribe(Arrays.asList(topic));
        initDBConnection();

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records) {
                  JSONObject obj = new JSONObject(record.value());
                  if (!obj.isNull("payload")){
                      if (obj.getJSONObject("payload").getString("op").equalsIgnoreCase("c")) {
                          mirrorInsertOperation(statement, obj.getJSONObject("payload"));
                      } else if(obj.getJSONObject("payload").getString("op").equalsIgnoreCase("u")){
                          mirrorUpdateOperation(statement, obj.getJSONObject("payload"));
                      } else if(obj.getJSONObject("payload").getString("op").equalsIgnoreCase("d")) {
                          mirrorDeleteOperation(statement, obj.getJSONObject("payload"));
                      } else {
                          throw new IllegalStateException("Oops, cannot process other than inset, update and delete query");
                      }
                  }
            }
        }
    }

    public static void initDBConnection() throws SQLException {
        conn = DriverManager.getConnection(DB_HOST, DB_USER, DB_PASSWORD);
        statement = conn.createStatement();
    }

    public static void mirrorInsertOperation(Statement statement, JSONObject payload) throws SQLException {
        String sql = "insert into animals_mirror(";
        JSONObject changedData = payload.getJSONObject("after");
        Set<String> keys = changedData.keySet();
        for (String key: keys) {
            sql = sql + key+", ";
        }
        sql = sql.substring(0, sql.length() -2)+") values (";
        for (String key: keys) {
            if (key.equals("id")){
                sql = sql +"'"+ changedData.getInt(key)+"'"+", ";
            } else {
                sql = sql +"'"+ changedData.getString(key)+"'"+", ";
            }
        }
        sql = sql.substring(0, sql.length() -2)+")";
        statement.executeUpdate(sql);
    }

    public static void mirrorUpdateOperation (Statement statement, JSONObject payload) throws SQLException {
        String sql = "update animals_mirror set ";
        JSONObject changedData = payload.getJSONObject("after");
        Set<String> keys = changedData.keySet();
        for (String key: keys) {
            if (key.equals("id")){
                sql = sql + key+"="+"'"+changedData.getInt("id")+"'"+", ";
            } else {
                sql = sql + key+"="+"'"+changedData.getString(key)+"'"+", ";
            }
        }
        sql = sql.substring(0, sql.length() -2)+" where id="+changedData.getInt("id");
        statement.executeUpdate(sql);
    }

    public static void mirrorDeleteOperation (Statement statement, JSONObject payload) throws SQLException {
        JSONObject deletedData = payload.getJSONObject("before");
        String sql = "delete from animals_mirror where id = '";
        sql = sql + deletedData.getInt("id")+"'";
        statement.executeUpdate(sql);
    }


}