package ru.chaika.streaming.simpleconsumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.chaika.streaming.common.model.EcommerceEvent;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * "Голый" Kafka consumer без Flink — читает события из топика ecommerce-events
 * в бесконечном цикле poll() и печатает их в консоль.
 * <p>
 * Цель этого класса — учебная: понять, что происходит "под капотом" у Flink,
 * прежде чем переходить на более высокоуровневый и надёжный Flink Kafka Source.
 * В частности, здесь видно вручную то, что Flink делает автоматически
 * через checkpointing: чтение батчами (poll) и подтверждение offset'ов (commit).
 */
public class SimpleConsumerApp {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ecommerce-events";

    // group.id — идентификатор consumer-группы. Kafka хранит offset'ы
    // (до какого места дочитаны партиции) отдельно для каждой группы.
    // Если запустить второй процесс с ТЕМ ЖЕ group.id, они поделят
    // партиции топика между собой (балансировка нагрузки).
    // Если запустить с ДРУГИМ group.id — второй процесс начнёт читать
    // весь топик заново, независимо от первого.
    private static final String GROUP_ID = "simple-consumer-group";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // ВАЖНО: отключаем автокоммит offset'ов. По умолчанию Kafka Consumer
        // сам коммитит offset каждые 5 секунд в фоне (enable.auto.commit=true) —
        // это удобно, но скрывает от нас сам механизм. Мы хотим закоммитить
        // offset вручную, ПОСЛЕ того как реально обработали (напечатали) сообщение —
        // это и есть разница между "at-most-once" (закоммитили раньше обработки,
        // рискуем потерять данные при падении) и "at-least-once" (коммитим после
        // обработки, рискуем обработать повторно при падении, но не потерять).
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // auto.offset.reset=earliest — если у группы ещё нет сохранённого offset'а
        // (первый запуск с этим group.id), начать читать топик с самого начала,
        // а не только новые сообщения, пришедшие после старта consumer'а.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ObjectMapper для обратного превращения JSON-строки из Kafka
        // в объект EcommerceEvent — зеркальная операция тому, что делает
        // JsonEventSerializer в producer'е.
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // try-with-resources закроет consumer корректно при выходе из блока
        // (в том числе через Ctrl+C, если JVM успеет обработать shutdown hook).
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // Подписываемся на топик. Список из одного элемента — можно подписаться
            // сразу на несколько топиков, тогда consumer будет читать из всех.
            consumer.subscribe(List.of(TOPIC));

            System.out.println("Consumer started, waiting for messages...");

            // Бесконечный цикл — сердце любого Kafka consumer'а.
            // Именно это Flink прячет от нас внутри Flink Kafka Source.
            while (true) {

                // poll() — блокирующий вызов, который либо возвращает пачку
                // новых сообщений (если они есть), либо ждёт до истечения таймаута
                // (здесь 1 секунда), если сообщений пока нет.
                // Обрати внимание: Kafka всегда отдаёт данные ПАЧКАМИ (батчами),
                // а не по одному сообщению за раз — это сделано для эффективности
                // (меньше сетевых round-trip'ов).
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    // Ничего не пришло за секунду — просто идём на следующую итерацию.
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EcommerceEvent event = objectMapper.readValue(record.value(), EcommerceEvent.class);

                        // Печатаем не только само событие, но и его "координаты" в Kafka —
                        // partition и offset — чтобы наглядно видеть механику:
                        // каждое сообщение имеет уникальный адрес (топик, партиция, offset).
                        System.out.printf(
                                "partition=%d offset=%d key=%s event=%s%n",
                                record.partition(), record.offset(), record.key(), event
                        );
                    } catch (Exception e) {
                        // Если JSON вдруг некорректный — не роняем весь consumer,
                        // а просто логируем проблему и идём дальше. В реальной системе
                        // сюда обычно добавляют отправку "плохого" сообщения
                        // в отдельный dead-letter topic для последующего разбора.
                        System.err.println("Failed to parse record at offset " + record.offset() + ": " + e.getMessage());
                    }
                }

                // Явный (ручной) коммит offset'ов — говорим Kafka: "я обработал
                // все сообщения из этого poll(), можешь запомнить, что я досюда дочитал".
                // Если процесс упадёт ДО этой строчки (например, во время печати события) —
                // при перезапуске эти же сообщения будут прочитаны заново (at-least-once).
                // Если бы мы коммитили offset ДО обработки — при падении часть сообщений
                // потерялась бы безвозвратно (at-most-once).
                consumer.commitSync();
            }
        }
    }
}