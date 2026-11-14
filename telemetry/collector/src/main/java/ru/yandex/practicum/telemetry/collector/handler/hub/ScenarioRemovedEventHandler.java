package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioRemovedEventAvro;

@Component
public class ScenarioRemovedEventHandler implements HubEventHandler {

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    public SpecificRecordBase mapToAvroPayload(HubEventProto event) {
        ScenarioRemovedEventProto scenarioRemoved = event.getScenarioRemoved();
        return ScenarioRemovedEventAvro.newBuilder()
                .setName(scenarioRemoved.getName())
                .build();
    }
}