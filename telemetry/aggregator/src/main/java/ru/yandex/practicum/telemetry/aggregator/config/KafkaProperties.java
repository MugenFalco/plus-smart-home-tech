package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Map;

@Getter
@Setter
@ConfigurationProperties(prefix = "aggregator.kafka")
public class KafkaProperties {

    private Topics topics;
    private Duration pollTimeout;
    private Map<String, String> consumer;
    private Map<String, String> producer;

    @Getter
    @Setter
    public static class Topics {

        private String sensors;
        private String snapshots;
    }
}