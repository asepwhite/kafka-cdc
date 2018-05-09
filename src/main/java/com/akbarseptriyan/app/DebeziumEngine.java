package com.akbarseptriyan.app;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class DebeziumEngine {

    private Configuration config;
    private Consumer notifyingFunction;
    private EmbeddedEngine debeziumEngine;

    public DebeziumEngine(Configuration config){
        this.config = config;
    }

    public void setCallbackFunction(Consumer notifyingFunction){
        this.notifyingFunction = notifyingFunction;
    }

    public void runEngine(){
        debeziumEngine = EmbeddedEngine.create().using(config).notifying(notifyingFunction).build();
        Executor executor = new DirectExecutor();
        executor.execute(debeziumEngine);
    }

    public void stopEngine(){
        debeziumEngine.stop();
    }

}

//Contoh konfigurasi
//
//    Configuration config = Configuration.create()
//            /* begin engine properties */
//            .with("connector.class",
//                    "io.debezium.connector.postgresql.PostgresConnector")
//            .with("offset.storage",
//                    "org.apache.kafka.connect.storage.FileOffsetBackingStore")
//            .with("offset.storage.file.filename",
//                    "/path/to/storage/offset.dat")
//            .with("offset.flush.interval.ms", 60000)
//            /* begin connector properties */
//            .with("name", "postgres-connector")
//            .with("database.hostname", "localhost")
//            .with("database.port", 5432)
//            .with("database.user", "dbz")
//            .with("database.password", "dbz12345")
//            .with("database.dbname", "testdb")
//            .with("database.server.name", "test")
//            .build();
