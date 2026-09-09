package ru.chaika.streaming.springconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Точка входа Spring Kafka consumer-приложения.
 * <p>
 * В отличие от simple-consumer (голый Kafka Consumer API с ручным poll()),
 * здесь весь цикл чтения/десериализации/коммита скрыт внутри Spring Kafka —
 * достаточно описать метод с @KafkaListener, остальное берёт на себя фреймворк.
 */
@SpringBootApplication
public class SpringConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SpringConsumerApplication.class, args);
    }
}