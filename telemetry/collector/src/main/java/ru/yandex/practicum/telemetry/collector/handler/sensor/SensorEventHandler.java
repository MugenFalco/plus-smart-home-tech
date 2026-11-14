package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventHandler {

    SensorEventProto.PayloadCase getMessageType();

    SpecificRecordBase mapToAvroPayload(SensorEventProto event);
}