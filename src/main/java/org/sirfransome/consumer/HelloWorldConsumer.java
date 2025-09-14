package org.sirfransome.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.sirfransome.avro.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class HelloWorldConsumer {

    private volatile boolean keepConsuming = true;
    private static Logger log = LoggerFactory.getLogger(HelloWorldConsumer.class);

    public static void main(String[] args) {
        System.out.println("Starting HelloWorldConsumer...");
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "kinaction_helloconsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        HelloWorldConsumer helloWorldConsumer = new HelloWorldConsumer();
        helloWorldConsumer.consume(props);

        Runtime.getRuntime().addShutdownHook(new Thread(helloWorldConsumer::shutdown));

    }

    private void consume(Properties props) {

        try (KafkaConsumer<Long, Alert> consumer = new KafkaConsumer<>(props)) {
            // subscribe to the topic
            consumer.subscribe(List.of("kinaction_schematest"));

            // poll for new data
            while (keepConsuming) {
                System.out.println("Polling for new data...");
                var records = consumer.poll(Duration.ofMillis(250));

                //loop through the records
                for (var record : records) {
                    log.info("kinaction_info offset = {}, kinaction_value = {}", record.offset(), record.value());
                    System.out.println("kinaction_info offset = " + record.offset() + ", kinaction_value = " + record.value());
                }
            }
        }
    }

    private void shutdown() {
        keepConsuming = false;
    }
}
