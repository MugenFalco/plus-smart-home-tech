package ru.yandex.practicum.telemetry.collector.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;

@Slf4j
@Service
public class LoggingCollectorService implements CollectorService {

    @Override
    public void collectSensorEvent(SensorEvent event) {
        log.debug("Collected sensor event: {}", event);
    }

    @Override
    public void collectHubEvent(HubEvent event) {
        log.debug("Collected hub event: {}", event);
    }
}