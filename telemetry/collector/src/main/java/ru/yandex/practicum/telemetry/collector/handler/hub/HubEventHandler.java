package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;

public interface HubEventHandler {

    HubEventType getMessageType();

    SpecificRecordBase mapToAvroPayload(HubEvent event);
}