package ru.chaika.streaming.common.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Модель одного события пользователя в интернет-магазине.
 * Соответствует одной строке исходного CSV-датасета
 * (REES46 eCommerce Behavior Data from multi category store).
 * <p>
 * Реализована как record — неизменяемый (immutable) объект-данные:
 * Java сама генерирует конструктор, геттеры (в стиле eventTime(), а не getEventTime()),
 * equals(), hashCode() и toString() на основе перечисленных полей.
 * Это удобно для событий в потоковой обработке — не нужно писать boilerplate-код,
 * а неизменяемость важна, так как события, once created, не должны меняться
 * по пути через Kafka -> Flink -> ClickHouse.
 *
 * @param eventTime    момент времени события в UTC (из поля event_time исходных данных)
 * @param eventType    тип события: просмотр / добавление в корзину / покупка
 * @param productId    идентификатор товара
 * @param categoryId   идентификатор категории товара (числовой код)
 * @param categoryCode человекочитаемый путь категории, например "electronics.smartphone".
 *                     Может быть null — не все товары в датасете имеют заполненную категорию.
 * @param brand        бренд товара. Может быть null — не у всех товаров указан бренд.
 * @param price        цена товара на момент события
 * @param userId       идентификатор пользователя, совершившего действие
 * @param userSession  идентификатор сессии пользователя (UUID),
 *                     позволяет сгруппировать события одного визита
 */
public record EcommerceEvent(
        Instant eventTime,
        EventType eventType,
        long productId,
        long categoryId,
        String categoryCode,
        String brand,
        BigDecimal price,
        long userId,
        String userSession
) {
}