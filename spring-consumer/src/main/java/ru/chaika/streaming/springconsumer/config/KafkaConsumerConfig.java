package ru.chaika.streaming.springconsumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import ru.chaika.streaming.common.model.EcommerceEvent;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация ConsumerFactory — объясняет Spring Kafka, как превращать
 * "сырые" байты сообщения из Kafka в объект EcommerceEvent.
 * <p>
 * Ключ сообщения десериализуем как обычную String (это userId),
 * а значение — через Spring-обёртку JsonDeserializer поверх Jackson,
 * которая сразу отдаёт готовый объект EcommerceEvent, а не строку JSON,
 * которую пришлось бы парсить вручную (как мы делали в simple-consumer).
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, EcommerceEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "spring-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // JsonDeserializer из Spring Kafka по умолчанию ожидает в заголовках
        // сообщения информацию о том, в какой именно Java-класс десериализовать
        // (Spring сам добавляет такой заголовок, если producer тоже был на Spring Kafka).
        // Но наш JSON пришёл от "чужого" JsonEventSerializer (обычный Jackson,
        // без Spring-заголовков) — поэтому явно указываем целевой класс
        // через VALUE_DEFAULT_TYPE, а заголовки игнорируем через TRUSTED_PACKAGES/USE_TYPE_INFO_HEADERS.
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ru.chaika.streaming.common.model");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EcommerceEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // Настраиваем ObjectMapper внутри JsonDeserializer так же,
        // как настраивали в JsonEventSerializer — нужен модуль для java.time.Instant.
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        JsonDeserializer<EcommerceEvent> jsonDeserializer =
                new JsonDeserializer<>(EcommerceEvent.class, objectMapper);

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                jsonDeserializer
        );
    }


    /**
     * Явно создаём фабрику контейнеров для @KafkaListener, привязанную
     * к нашей кастомной consumerFactory() (с JSON-десериализацией в EcommerceEvent).
     * <p>
     * Без этого бина Spring Boot использует свою автоконфигурированную фабрику,
     * основанную только на настройках из application.yml — а там нет информации
     * о том, что value нужно десериализовать именно в EcommerceEvent через JSON,
     * поэтому используется StringDeserializer по умолчанию.
     * <p>
     * Имя бина "kafkaListenerContainerFactory" — стандартное имя, которое
     * @KafkaListener ищет по умолчанию, если explicit containerFactory
     * не указан в самой аннотации.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EcommerceEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, EcommerceEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Явно включаем ручное подтверждение (Acknowledgment) в самом коде,
        // а не только через application.yml — для надёжности, дублируем настройку.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }

}