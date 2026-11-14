package ru.yandex.practicum.telemetry.collector.handler.hub;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class HubEventHandlerRegistry {

    private final Map<HubEventType, HubEventHandler> handlers;

    public HubEventHandlerRegistry(Set<HubEventHandler> handlerSet) {
        this.handlers = new HashMap<>();
        for (HubEventHandler handler : handlerSet) {
            handlers.put(handler.getMessageType(), handler);
        }
    }

    public HubEventHandler get(HubEventType type) {
        HubEventHandler handler = handlers.get(type);
        if (handler == null) {
            throw new IllegalArgumentException("Не найден хендлер для типа события хаба: " + type);
        }
        return handler;
    }
}