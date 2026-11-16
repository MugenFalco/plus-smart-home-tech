package ru.yandex.practicum.telemetry.aggregator.config;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.util.Properties;

@Configuration
public class KafkaClientConfig {

    @Bean(destroyMethod = "")
    public KafkaConsumer<String, SensorEventAvro> sensorEventConsumer(KafkaProperties kafkaProperties) {
        Properties config = new Properties();
        config.putAll(kafkaProperties.getConsumer());
        return new KafkaConsumer<>(config);
    }

    @Bean(destroyMethod = "")
    public KafkaProducer<String, SpecificRecordBase> snapshotProducer(KafkaProperties kafkaProperties) {
        Properties config = new Properties();
        config.putAll(kafkaProperties.getProducer());
        return new KafkaProducer<>(config);
    }
}