package ru.chaika.streaming.webproducer.web;

/**
 * Тело POST-запроса от веб-формы. Все поля приходят как строки —
 * так проще с обычной HTML-формой без сложной JS-валидации типов,
 * преобразование в нужные типы (long, BigDecimal) происходит
 * уже в EventController при сборке EcommerceEvent.
 */
public record ManualEventRequest(
        String eventType,
        String productId,
        String categoryId,
        String categoryCode,
        String brand,
        String price,
        String userId,
        String userSession
) {
}