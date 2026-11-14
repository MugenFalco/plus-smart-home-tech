package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;

@Component
public class MotionSensorEventHandler implements SensorEventHandler {

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(SensorEventProto event) {
        MotionSensorProto motionSensor = event.getMotionSensor();
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(motionSensor.getLinkQuality())
                .setMotion(motionSensor.getMotion())
                .setVoltage(motionSensor.getVoltage())
                .build();
    }
}