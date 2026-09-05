package ru.chaika.streaming.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Тип события пользователя в интернет-магазине.
 * Соответствует полю event_type в исходном датасете (REES46 eCommerce Behavior Data).
 * <p>
 * Возможные значения в сыром CSV: "view", "cart", "purchase".
 */
public enum EventType {

    /** Пользователь просмотрел карточку товара. */
    VIEW("view"),

    /** Пользователь добавил товар в корзину. */
    CART("cart"),

    /** Пользователь совершил покупку товара. */
    PURCHASE("purchase");

    /**
     * Исходное строковое значение из CSV/JSON,
     * ровно в том виде, в котором оно встречается в данных.
     * Используется вместо имени enum-константы (VIEW/CART/PURCHASE),
     * так как имена в Java традиционно пишутся в верхнем регистре,
     * а в датасете и в JSON значения — в нижнем.
     */
    private final String rawValue;

    EventType(String rawValue) {
        this.rawValue = rawValue;
    }

    /**
     * Указывает Jackson, что при сериализации enum в JSON
     * нужно использовать это значение (например, "view"),
     * а не имя константы по умолчанию (например, "VIEW").
     */
    @JsonValue
    public String getRawValue() {
        return rawValue;
    }

    /**
     * Обратное преобразование: из строки (как в CSV/JSON) — в enum-константу.
     * Аннотация @JsonCreator говорит Jackson использовать именно этот метод
     * при десериализации JSON в EventType.
     *
     * @param rawValue строковое значение, например "view"
     * @return соответствующая константа EventType
     * @throws IllegalArgumentException если значение не соответствует ни одному известному типу события
     */
    @JsonCreator
    public static EventType fromRawValue(String rawValue) {
        for (EventType type : values()) {
            if (type.rawValue.equals(rawValue)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type: " + rawValue);
    }
}