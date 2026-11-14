package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;

@Component
public class DeviceAddedEventHandler implements HubEventHandler {

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(HubEventProto event) {
        DeviceAddedEventProto deviceAdded = event.getDeviceAdded();
        return DeviceAddedEventAvro.newBuilder()
                .setId(deviceAdded.getId())
                .setType(DeviceTypeAvro.valueOf(deviceAdded.getType().name()))
                .build();
    }
}