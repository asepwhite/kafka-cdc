package com.akbarseptriyan.app;
import java.sql.*;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.connect.source.SourceTask;
import org.json.*;

public class DBCount {

    private static final String JBDC_DRIVER = "org.postgresql.Driver";
    private static final String DB_HOST = "jdbc:postgresql://localhost:5432/debezdb";
    private static final String DB_USER = "debezium";
    private static final String DB_PASSWORD = "dbz12345";

    private static Connection conn;
    private static Statement statement;


    public static void main (String[] args) throws SQLException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-2");
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
                        countInsertOperation(statement, obj.getJSONObject("payload"));
                    } else if(obj.getJSONObject("payload").getString("op").equalsIgnoreCase("u")){
                        countUpdateOperation(statement, obj.getJSONObject("payload"));
                    } else if(obj.getJSONObject("payload").getString("op").equalsIgnoreCase("d")) {
                        countDeleteOperation(statement, obj.getJSONObject("payload"));
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

     public static void upsert(Statement statement, String type, int total) throws SQLException {
         String sql = "select * from animals_count where type = ";
         sql = sql+"'"+type+"'";
         ResultSet rs = statement.executeQuery(sql);

        if (rs.next()){
            sql = "update animals_count set total = ";
            sql = sql + "'"+total+"' where type = ";
            sql = sql + "'"+type+"'";
            statement.executeUpdate(sql);
        } else {
            sql = "insert into animals_count(type, total) values (";
            sql = sql + "'"+type+"', "+"'"+total+"')";
            statement.executeUpdate(sql);
        }
     }

    public static void updel(Statement statement, String type, int total) throws SQLException {
        String sql = "";
        if (total !=  0){
            sql = "update animals_count set total = ";
            sql = sql + "'"+total+"' where type = ";
            sql = sql + "'"+type+"'";
            statement.executeUpdate(sql);
        } else {
            sql = "delete from animals_count where type = ";
            sql = sql + "'"+type+"'";
            statement.executeUpdate(sql);
        }
    }

    public static void countInsertOperation(Statement statement, JSONObject payload) throws SQLException {
        String sql = "select count (type) as total from animals where type = ";
        JSONObject changedData = payload.getJSONObject("after");
        sql = sql+"'"+changedData.getString("type")+"'";
        ResultSet rs  = statement.executeQuery(sql);
        rs.next();
        upsert(statement, changedData.getString("type"), rs.getInt("total"));
    }

    public static void countUpdateOperation(Statement statement, JSONObject payload) throws SQLException {
        String sql = "select count (type) as total from animals where type = ";
        JSONObject changedData = payload.getJSONObject("after");
        sql = sql+"'"+changedData.getString("type")+"'";
        ResultSet rs  = statement.executeQuery(sql);
        rs.next();
        upsert(statement, changedData.getString("type"), rs.getInt("total"));
        changedData = payload.getJSONObject("before");
        sql = "select count (type) as total from animals where type = ";
        sql = sql+"'"+changedData.getString("type")+"'";
        rs  = statement.executeQuery(sql);
        rs.next();
        updel(statement, changedData.getString("type"), rs.getInt("total"));
    }

    public static void countDeleteOperation(Statement statement, JSONObject payload) throws SQLException {
        String sql = "select count (type) as total from animals where type = ";
        JSONObject changedData = payload.getJSONObject("before");
        sql = sql+"'"+changedData.getString("type")+"'";
        ResultSet rs  = statement.executeQuery(sql);
        rs.next();
        updel(statement, changedData.getString("type"), rs.getInt("total"));
    }

}