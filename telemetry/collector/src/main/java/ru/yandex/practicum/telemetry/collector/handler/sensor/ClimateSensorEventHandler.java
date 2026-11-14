package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;

@Component
public class ClimateSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(SensorEventProto event) {
        ClimateSensorProto climateSensor = event.getClimateSensor();
        return ClimateSensorAvro.newBuilder()
                .setTemperatureC(climateSensor.getTemperatureC())
                .setHumidity(climateSensor.getHumidity())
                .setCo2Level(climateSensor.getCo2Level())
                .build();
    }
}