package org.sirfransome.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class HelloWorldConsumer {

    private volatile boolean keepConsuming = true;
    private static Logger log = LoggerFactory.getLogger(HelloWorldConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092,localhost:29093,localhost:29094"); // Docker ports not localhost
        props.put("group.id", "kinaction_helloconsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        HelloWorldConsumer helloWorldConsumer = new HelloWorldConsumer();
        helloWorldConsumer.consume(props);

        Runtime.getRuntime().addShutdownHook(new Thread(helloWorldConsumer::shutdown));

    }

    private void consume(Properties props) {

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // subscribe to the topic
            consumer.subscribe(List.of("kinaction_helloworld"));

            // poll for new data
            while (keepConsuming) {
                var records = consumer.poll(Duration.ofMillis(250));

                //loop through the records
                for (var record : records) {
                    log.info("kinaction_info offset = {}, kinaction_value = {}", record.offset(), record.value());
                }
            }
        }
    }

    private void shutdown() {
        keepConsuming = false;
    }
}
