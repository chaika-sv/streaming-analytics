package ru.chaika.streaming.webproducer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.chaika.streaming.common.kafka.KafkaEventProducer;

/**
 * Spring-конфигурация, создающая единственный (singleton) экземпляр
 * KafkaEventProducer на всё приложение — переиспользуем одно соединение
 * с Kafka для всех входящих HTTP-запросов, а не создаём новое на каждый запрос.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.manual-events-topic}")
    private String topic;

    @Bean
    public KafkaEventProducer kafkaEventProducer() {
        return new KafkaEventProducer(bootstrapServers, topic);
    }
}