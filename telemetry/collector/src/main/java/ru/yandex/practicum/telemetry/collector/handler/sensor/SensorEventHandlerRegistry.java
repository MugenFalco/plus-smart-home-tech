package ru.yandex.practicum.telemetry.collector.handler.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class SensorEventHandlerRegistry {

    private final Map<SensorEventType, SensorEventHandler> handlers;

    public SensorEventHandlerRegistry(Set<SensorEventHandler> handlerSet) {
        this.handlers = new HashMap<>();
        for (SensorEventHandler handler : handlerSet) {
            handlers.put(handler.getMessageType(), handler);
        }
    }

    public SensorEventHandler get(SensorEventType type) {
        SensorEventHandler handler = handlers.get(type);
        if (handler == null) {
            throw new IllegalArgumentException("Не найден хендлер для типа события датчика: " + type);
        }
        return handler;
    }
}