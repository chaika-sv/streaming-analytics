package ru.chaika.streaming.producer.parser;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import ru.chaika.streaming.common.model.EcommerceEvent;
import ru.chaika.streaming.common.model.EventType;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Потоковый парсер CSV-файла с событиями интернет-магазина.
 * <p>
 * Ключевая идея: файл читается построчно и каждая строка сразу же
 * передаётся вызывающему коду через callback (EventConsumer),
 * а не накапливается в памяти в виде списка. Это важно, так как
 * исходный датасет может весить гигабайты — загрузка всего файла
 * в List<EcommerceEvent> привела бы к OutOfMemoryError.
 */
public final class CsvEventParser {

    // Приватный конструктор — класс содержит только статический метод parse()
    // и не предназначен для создания экземпляров.
    private CsvEventParser() {
    }

    /**
     * Читает CSV-файл построчно и вызывает consumer.accept(event)
     * для каждой строки данных (кроме заголовка).
     *
     * @param filePath путь к CSV-файлу на диске
     * @param consumer callback, вызываемый для каждого распарсенного события
     * @throws IOException если файл не найден, повреждён,
     *                      или строка CSV не соответствует ожидаемому формату
     */
    public static void parse(String filePath, EventConsumer consumer) throws IOException {
        // try-with-resources гарантирует, что CSVReader (а с ним и FileReader)
        // будет закрыт автоматически, даже если внутри цикла произойдёт исключение.
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {

            // Первая строка файла — заголовок (event_time, event_type,...).
            // Считываем и просто отбрасываем — она нам не нужна для данных,
            // а порядок колонок уже жёстко зашит в метод toEvent() ниже.
            String[] header = reader.readNext();

            String[] row;
            // readNext() возвращает null, когда файл закончился — это условие выхода из цикла.
            while ((row = reader.readNext()) != null) {
                EcommerceEvent event = toEvent(row);
                consumer.accept(event);
            }
        } catch (CsvValidationException e) {
            // OpenCSV выбрасывает свой checked exception при некорректном формате строки
            // (например, незакрытая кавычка). Оборачиваем в IOException,
            // чтобы у вызывающего кода была одна точка обработки ошибок ввода-вывода.
            throw new IOException("Failed to parse CSV", e);
        }
    }

    /**
     * Преобразует одну строку CSV (массив строковых полей) в объект EcommerceEvent.
     * <p>
     * Индексы полей жёстко привязаны к порядку колонок в исходном датасете:
     * event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session
     *
     * @param row строки-значения одной строки CSV, в порядке колонок исходного файла
     * @return распарсенное событие
     */
    private static EcommerceEvent toEvent(String[] row) {
        return new EcommerceEvent(
                EventTimeParser.parse(row[0]),
                EventType.fromRawValue(row[1]),
                Long.parseLong(row[2]),
                Long.parseLong(row[3]),
                // category_code и brand могут быть пустыми строками в исходном CSV
                // (не все товары имеют заполненную категорию/бренд).
                // Заменяем пустую строку на null, чтобы явно отличать
                // "поле отсутствует" от случайной пустой строки.
                row[4].isEmpty() ? null : row[4],
                row[5].isEmpty() ? null : row[5],
                new BigDecimal(row[6]),
                Long.parseLong(row[7]),
                row[8]
        );
    }

    /**
     * Функциональный интерфейс для обработки одного события за раз.
     * Используется как callback в parse() — позволяет вызывающему коду
     * решить, что делать с событием (напечатать, отправить в Kafka и т.д.),
     * не заставляя CsvEventParser знать об этом.
     */
    @FunctionalInterface
    public interface EventConsumer {
        /**
         * Обрабатывает одно событие.
         *
         * @param event распарсенное событие
         * @throws IOException если обработка события связана с вводом-выводом
         *                      (например, отправкой в сеть) и может завершиться ошибкой
         */
        void accept(EcommerceEvent event) throws IOException;
    }
}