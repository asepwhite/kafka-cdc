package com.akbarseptriyan.app;

//import org.apache.kafka.clients.consumer.Consumer;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;

import java.util.concurrent.Executor;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
//        ConsumerTest kafkaConsumer = new ConsumerTest();
//        kafkaConsumer.run();
        // Define the configuration for the embedded and MySQL connector ...
//        Configuration config = Configuration.create()
//                /* begin engine properties */
//                .with("connector.class",
//                        "io.debezium.connector.mysql.MySqlConnector")
//                .with("offset.storage",
//                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
//                .with("offset.storage.file.filename",
//                        "/path/to/storage/offset.dat")
//                .with("offset.flush.interval.ms", 60000)
//                /* begin connector properties */
//                .with("name", "my-sql-connector")
//                .with("database.hostname", "localhost")
//                .with("database.port", 3306)
//                .with("database.user", "root")
//                .with("database.password", "rootroot")
//                .with("server.id", 85744)
//                .with("database.server.name", "my-app-connector")
//                .with("database.history",
//                        "io.debezium.relational.history.FileDatabaseHistory")
//                .with("database.history.file.filename",
//                        "/Users/akbarseptriyan/Sites/garbage/dbhistory.dat")
//                .build();

        Configuration config = Configuration.create()
                /* begin engine properties */
                .with("connector.class",
                        "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename",
                        "/path/to/storage/offset.dat")
                .with("offset.flush.interval.ms", 60000)
                /* begin connector properties */
                .with("name", "postgres-connector")
                .with("database.hostname", "localhost")
                .with("database.port", 5432)
                .with("database.user", "dbz")
                .with("database.password", "dbz12345")
                .with("database.dbname", "testdb")
                .with("database.server.name", "test")
                .build();



// Create the engine with this configuration ...
        DbzFunction lala = new DbzFunction();
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(lala)
                .build();

// Run the engine asynchronously ...
        Executor executor = new DirectExecutor();
        executor.execute(engine);

// At some later time ...
//        engine.stop();
    }
}
