package ru.yandex.practicum.telemetry.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.KafkaProperties;
import ru.yandex.practicum.telemetry.aggregator.service.SnapshotAggregator;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final SnapshotAggregator snapshotAggregator;
    private final KafkaProperties kafkaProperties;

    public void start() {
        registerShutdownHook();

        try {
            consumer.subscribe(List.of(kafkaProperties.getTopics().getSensors()));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(kafkaProperties.getPollTimeout());

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    snapshotAggregator.updateState(record.value()).ifPresent(this::sendSnapshot);
                }

                if (!records.isEmpty()) {
                    consumer.commitAsync();
                }
            }
        } catch (WakeupException ignored) {
            // штатное завершение: консьюмер и продюсер закрываются в блоке finally
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private void registerShutdownHook() {
        Thread pollingThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                pollingThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void sendSnapshot(SensorsSnapshotAvro snapshot) {
        String topic = kafkaProperties.getTopics().getSnapshots();
        ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(
                topic, null, snapshot.getTimestamp().toEpochMilli(), snapshot.getHubId(), snapshot);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Не удалось отправить снапшот хаба {} в топик {}", snapshot.getHubId(), topic, exception);
            } else {
                log.debug("Снапшот хаба {} отправлен в топик {} [partition={}, offset={}]",
                        snapshot.getHubId(), topic, metadata.partition(), metadata.offset());
            }
        });
    }
}