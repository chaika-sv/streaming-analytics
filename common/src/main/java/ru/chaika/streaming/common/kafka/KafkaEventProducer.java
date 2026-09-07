package ru.chaika.streaming.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.chaika.streaming.common.model.EcommerceEvent;

import java.util.Properties;

/**
 * Обёртка над стандартным KafkaProducer, которая инкапсулирует
 * конфигурацию клиента и сериализацию событий в JSON.
 * <p>
 * Реализует AutoCloseable, чтобы можно было использовать
 * в try-with-resources и гарантированно закрывать соединение с Kafka.
 */
public final class KafkaEventProducer implements AutoCloseable {

    private final KafkaProducer<String, String> kafkaProducer;
    private final JsonEventSerializer serializer;
    private final String topic;

    /**
     * @param bootstrapServers адрес(а) Kafka-брокера, например "localhost:9092"
     * @param topic            имя топика, в который будут отправляться события
     */
    public KafkaEventProducer(String bootstrapServers, String topic) {
        this.topic = topic;
        this.serializer = new JsonEventSerializer();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Ключ и значение сообщения передаются в Kafka как byte[].
        // StringSerializer превращает Java String в байты (UTF-8) —
        // используем его и для ключа (userId), и для значения (JSON события).
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ACKS_CONFIG=all — продюсер ждёт подтверждения записи от всех in-sync реплик,
        // а не только от лидера партиции. Для локального одноброкерного кластера
        // это не даёт дополнительной надёжности (реплик всего одна),
        // но это правильная практика "на будущее", если кластер станет многоузловым.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.kafkaProducer = new KafkaProducer<>(props);
    }

    /**
     * Отправляет одно событие в Kafka.
     * <p>
     * В качестве ключа сообщения используется userId события (в виде строки).
     * Это важно: Kafka гарантирует порядок сообщений только внутри одной партиции,
     * а сообщения с одинаковым ключом всегда попадают в одну и ту же партицию.
     * Таким образом, все события одного пользователя будут обработаны по порядку —
     * это пригодится позже, например, для подсчёта сессий во Flink.
     *
     * @param event событие для отправки
     */
    public void send(EcommerceEvent event) {
        String key = String.valueOf(event.userId());
        String value = serializer.serialize(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        // send() асинхронный — сообщение уходит в буфер клиента и отправляется
        // фоновым потоком. Явно не блокируемся на send().get(), чтобы не терять
        // throughput — это осознанный trade-off для пет-проекта:
        // при ошибке отправки колбэк просто залогирует проблему, не остановив весь поток.
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send event: " + exception.getMessage());
            }
        });
    }

    /**
     * Закрывает соединение с Kafka, дожидаясь отправки всех буферизованных сообщений.
     */
    @Override
    public void close() {
        // close() без аргументов ждёт до 30 секунд (значение по умолчанию)
        // отправки всех сообщений, накопленных в буфере, перед закрытием соединения.
        kafkaProducer.close();
    }
}