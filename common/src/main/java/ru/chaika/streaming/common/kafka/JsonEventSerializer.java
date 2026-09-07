package ru.chaika.streaming.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ru.chaika.streaming.common.model.EcommerceEvent;

/**
 * Отвечает за преобразование EcommerceEvent в JSON-строку
 * перед отправкой в Kafka. Kafka сама по себе не знает о Java-объектах,
 * она работает с byte[] — поэтому нужен явный шаг сериализации.
 */
public final class JsonEventSerializer {

    // ObjectMapper — основной класс Jackson для сериализации/десериализации JSON.
    // Создаём один экземпляр и переиспользуем его (он потокобезопасен после конфигурации),
    // а не создаём новый на каждый вызов — это дорогая операция.
    private final ObjectMapper objectMapper;

    public JsonEventSerializer() {
        this.objectMapper = new ObjectMapper();

        // Без этого модуля Jackson не умеет сериализовать java.time.Instant
        // (и другие классы из java.time) и выбросит исключение при попытке.
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Сериализует событие в JSON-строку.
     *
     * @param event событие для сериализации
     * @return JSON-представление события
     */
    public String serialize(EcommerceEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            // JsonProcessingException — checked exception, оборачиваем в unchecked,
            // так как ошибка сериализации собственного record'а с простыми полями
            // — это, по сути, программная ошибка (баг), а не ожидаемая ситуация
            // на уровне вызывающего кода, которую стоит явно обрабатывать.
            throw new RuntimeException("Failed to serialize event to JSON: " + event, e);
        }
    }
}