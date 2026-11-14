package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.model.sensor.TemperatureSensorEvent;

@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventType getMessageType() {
        return SensorEventType.TEMPERATURE_SENSOR_EVENT;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(SensorEvent event) {
        TemperatureSensorEvent temperatureEvent = (TemperatureSensorEvent) event;
        return TemperatureSensorAvro.newBuilder()
                .setId(temperatureEvent.getId())
                .setHubId(temperatureEvent.getHubId())
                .setTimestamp(temperatureEvent.getTimestamp())
                .setTemperatureC(temperatureEvent.getTemperatureC())
                .setTemperatureF(temperatureEvent.getTemperatureF())
                .build();
    }
}