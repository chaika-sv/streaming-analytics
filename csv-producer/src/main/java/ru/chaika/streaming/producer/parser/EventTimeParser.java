package ru.chaika.streaming.producer.parser;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Утилита для парсинга поля event_time из исходного CSV-датасета.
 * <p>
 * Формат в данных: "2019-10-01 00:00:00 UTC" — то есть дата и время
 * без смещения таймзоны в стандартном ISO-виде, а с буквальным текстовым
 * суффиксом " UTC" в конце строки. Из-за этого нельзя напрямую использовать
 * Instant.parse() — он ожидает строгий ISO-8601 формат (например, "2019-10-01T00:00:00Z").
 */
public final class EventTimeParser {

    /**
     * Формат даты/времени без учёта таймзоны — используется для парсинга
     * только той части строки, что идёт до суффикса " UTC".
     */
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Приватный конструктор — класс содержит только статические методы
    // и не должен создаваться как объект (utility-класс).
    private EventTimeParser() {
    }

    /**
     * Парсит строку вида "2019-10-01 00:00:00 UTC" в Instant.
     *
     * @param rawEventTime исходная строка из CSV, колонка event_time
     * @return момент времени в виде Instant (точка на шкале UTC)
     */
    public static Instant parse(String rawEventTime) {
        // Суффикс " UTC" всегда занимает ровно 4 символа (пробел + 3 буквы),
        // поэтому просто отрезаем последние 4 символа строки,
        // не прибегая к более сложному/медленному regex или split().
        String withoutZone = rawEventTime.substring(0, rawEventTime.length() - 4);

        // Парсим оставшуюся часть как "наивную" дату-время без таймзоны.
        LocalDateTime localDateTime = LocalDateTime.parse(withoutZone, FORMATTER);

        // Явно указываем, что эта дата-время относится к зоне UTC (смещение 0),
        // и получаем Instant — точку на абсолютной шкале времени,
        // не зависящую от локали или часового пояса.
        return localDateTime.toInstant(ZoneOffset.UTC);
    }
}