package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.config.KafkaTopicsProperties;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandlerRegistry;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandlerRegistry;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaCollectorService implements CollectorService {

    private final KafkaProducer<String, SpecificRecordBase> kafkaProducer;
    private final KafkaTopicsProperties topicsProperties;
    private final SensorEventHandlerRegistry sensorHandlerRegistry;
    private final HubEventHandlerRegistry hubHandlerRegistry;

    @Override
    public void collectSensorEvent(SensorEvent event) {
        SensorEventHandler handler = sensorHandlerRegistry.get(event.getType());

        SensorEventAvro avroEvent = SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(handler.mapToAvroPayload(event))
                .build();

        send(topicsProperties.getSensors(), event.getHubId(), avroEvent);
    }

    @Override
    public void collectHubEvent(HubEvent event) {
        HubEventHandler handler = hubHandlerRegistry.get(event.getType());

        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(handler.mapToAvroPayload(event))
                .build();

        send(topicsProperties.getHubs(), event.getHubId(), avroEvent);
    }

    private void send(String topic, String key, SpecificRecordBase avroEvent) {
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(topic, key, avroEvent);

        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить событие в топик {}: {}", topic, exception.getMessage(), exception);
            } else {
                log.debug("Событие отправлено в топик {} [partition={}, offset={}]",
                        topic, metadata.partition(), metadata.offset());
            }
        });
    }
}