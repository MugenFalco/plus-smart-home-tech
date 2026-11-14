package ru.yandex.practicum.telemetry.collector.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.serializer.GeneralAvroSerializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopicsProperties;

import java.time.Instant;
import java.util.Properties;

@Slf4j
@Component
public class EventKafkaProducer implements AutoCloseable {

    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final KafkaTopicsProperties topics;

    public EventKafkaProducer(KafkaTopicsProperties topics,
                              @Value("${collector.kafka.bootstrap-servers}") String bootstrapServers) {
        this.topics = topics;

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GeneralAvroSerializer.class);
        this.producer = new KafkaProducer<>(config);
    }

    public void sendSensorEvent(SensorEventAvro event) {
        send(topics.getSensors(), event.getHubId(), event.getTimestamp(), event);
    }

    public void sendHubEvent(HubEventAvro event) {
        send(topics.getHubs(), event.getHubId(), event.getTimestamp(), event);
    }

    private void send(String topic, String key, Instant timestamp, SpecificRecordBase avroEvent) {
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, avroEvent);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие в топик {}: {}", topic, exception.getMessage(), exception);
            } else {
                log.debug("Событие отправлено в топик {} [partition={}, offset={}]",
                        topic, metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        producer.close();
    }
}