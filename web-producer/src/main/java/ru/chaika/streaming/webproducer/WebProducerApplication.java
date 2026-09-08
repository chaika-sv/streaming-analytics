package ru.chaika.streaming.webproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Точка входа веб-приложения для ручной отправки тестовых событий в Kafka.
 * Отдельный от основного CSV-producer'а модуль, чтобы можно было
 * вручную создавать произвольные события через простую веб-форму,
 * не трогая поток данных из датасета.
 */
@SpringBootApplication
public class WebProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(WebProducerApplication.class, args);
    }
}