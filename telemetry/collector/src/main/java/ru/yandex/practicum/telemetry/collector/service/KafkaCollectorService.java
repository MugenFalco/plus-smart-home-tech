package ru.yandex.practicum.telemetry.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.handler.hub.HubEventHandlerRegistry;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandler;
import ru.yandex.practicum.telemetry.collector.handler.sensor.SensorEventHandlerRegistry;
import ru.yandex.practicum.telemetry.collector.kafka.EventKafkaProducer;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;

@Service
@RequiredArgsConstructor
public class KafkaCollectorService implements CollectorService {

    private final EventKafkaProducer producer;
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

        producer.sendSensorEvent(avroEvent);
    }

    @Override
    public void collectHubEvent(HubEvent event) {
        HubEventHandler handler = hubHandlerRegistry.get(event.getType());

        HubEventAvro avroEvent = HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(handler.mapToAvroPayload(event))
                .build();

        producer.sendHubEvent(avroEvent);
    }
}