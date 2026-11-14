package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;
import ru.yandex.practicum.telemetry.collector.mapper.TimestampMapper;

@Component
public class TemperatureSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(SensorEventProto event) {
        TemperatureSensorProto temperatureSensor = event.getTemperatureSensor();
        return TemperatureSensorAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(TimestampMapper.toInstant(event.getTimestamp()))
                .setTemperatureC(temperatureSensor.getTemperatureC())
                .setTemperatureF(temperatureSensor.getTemperatureF())
                .build();
    }
}