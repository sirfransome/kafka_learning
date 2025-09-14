package org.sirfransome.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sirfransome.avro.Alert;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.sirfransome.avro.AlertStatus.Critical;

@Slf4j
public class HelloWorldProducer {

    public static void main(String[] args) {
        System.out.println("kinaction_info HelloWorldProducer started");
        Properties kaProperties = new Properties();
        kaProperties.put("bootstrap.servers",
                "localhost:9092,localhost:9093,localhost:9094");
        kaProperties.put("key.serializer",
                "org.apache.kafka.common.serialization.LongSerializer");
        kaProperties.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        kaProperties.put("schema.registry.url",
                "http://localhost:8081");
        try (Producer<Long, Alert> producer =
                     new KafkaProducer<>(kaProperties)) {
            Alert alert =
                    new Alert(12345L,
                            Instant.now().toEpochMilli(),
                            Critical);
            log.info("kinaction_info Alert -> {}", alert);
            System.out.println("kinaction_info Alert -> " + alert);

            ProducerRecord<Long, Alert> producerRecord =
                    new ProducerRecord<>("kinaction_schematest",
                            alert.getSensorId(),
                            alert);
            producer.send(producerRecord).get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
