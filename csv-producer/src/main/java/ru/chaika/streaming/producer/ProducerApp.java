package ru.chaika.streaming.producer;

import ru.chaika.streaming.common.kafka.KafkaEventProducer;
import ru.chaika.streaming.producer.parser.CsvEventParser;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Точка входа producer-приложения.
 * <p>
 * Читает CSV-файл с событиями и отправляет их в Kafka с заданной скоростью (RPS —
 * requests per second), эмулируя тем самым "живой" поток данных из статичного файла.
 */
public class ProducerApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ecommerce-events";
    private static final String CSV_FILE_PATH = "csv-producer/src/main/resources/data/sample.csv";

    // Целевая скорость отправки — событий в секунду.
    // Подобрано как разумный старт для локального одноброкерного кластера:
    // достаточно быстро, чтобы не ждать долго, но достаточно медленно,
    // чтобы вручную наблюдать за потоком в Kafka UI в реальном времени.
    private static final int TARGET_EVENTS_PER_SECOND = 50;

    public static void main(String[] args) throws IOException, InterruptedException {
        // Вычисляем задержку между отправками в миллисекундах,
        // чтобы суммарно получалось примерно TARGET_EVENTS_PER_SECOND событий в секунду.
        long delayBetweenEventsMs = 1000L / TARGET_EVENTS_PER_SECOND;

        // Счётчик отправленных событий. Используем AtomicLong (а не обычный long),
        // так как переменная изменяется внутри лямбды ниже — Java требует,
        // чтобы захваченные лямбдой локальные переменные были effectively final,
        // а AtomicLong позволяет менять значение внутри объекта,
        // не переприсваивая саму ссылку на переменную.
        AtomicLong sentCount = new AtomicLong(0);

        System.out.println("Starting producer: target rate = " + TARGET_EVENTS_PER_SECOND + " events/sec");

        // try-with-resources гарантирует вызов kafkaEventProducer.close()
        // даже если внутри блока произойдёт исключение — то есть все
        // накопленные в буфере сообщения будут отправлены перед завершением.
        try (KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(BOOTSTRAP_SERVERS, TOPIC)) {

            CsvEventParser.parse(CSV_FILE_PATH, event -> {
                kafkaEventProducer.send(event);
                long count = sentCount.incrementAndGet();

                // Логируем прогресс не на каждое событие (это замедлило бы вывод
                // и засорило консоль), а раз в 1000 событий.
                if (count % 1000 == 0) {
                    System.out.println("Sent " + count + " events");
                }

                // Искусственная задержка — именно она превращает "мгновенное"
                // чтение файла в равномерный поток с заданным RPS.
                try {
                    Thread.sleep(delayBetweenEventsMs);
                } catch (InterruptedException e) {
                    // Восстанавливаем флаг прерывания потока (хорошая практика
                    // при перехвате InterruptedException) и пробрасываем дальше
                    // как IOException, так как EventConsumer.accept() объявляет
                    // именно этот checked exception.
                    Thread.currentThread().interrupt();
                    throw new IOException("Producer interrupted", e);
                }
            });
        }

        System.out.println("Done. Total events sent: " + sentCount.get());
    }
}