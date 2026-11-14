package ru.yandex.practicum.telemetry.collector.mapper;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public final class TimestampMapper {

    private TimestampMapper() {
    }

    public static Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}