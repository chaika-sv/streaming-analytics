package ru.chaika.streaming.flinkjob;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import ru.chaika.streaming.common.model.EcommerceEvent;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Первый (учебный) Flink job: читает события из Kafka, группирует по типу события
 * (view/cart/purchase) внутри тумблинг-окна в 1 минуту, считает количество
 * событий каждого типа и печатает результат в консоль.
 * <p>
 * Это самый простой возможный pipeline — цель здесь не финальная бизнес-логика,
 * а проверить, что вся цепочка Kafka -> Flink -> вывод в принципе работает,
 * прежде чем усложнять агрегацию (топ-N категорий) и sink в ClickHouse.
 */
public class EventTypeCountJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "manual-events";

    // Форматтер для читаемого вывода времени окна. Используем UTC явно,
    // так как event_time в датасете тоже в UTC (мы это учли ещё в EventTimeParser) —
    // иначе форматирование по умолчанию взяло бы локальную таймзону системы,
    // и цифры бы не совпадали с исходными данными.
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {

        // StreamExecutionEnvironment — точка входа в Flink DataStream API.
        // Это как "SparkContext" в Spark или "точка сборки конвейера" —
        // через него описывается весь pipeline: откуда читаем, что делаем,
        // куда пишем. Сам pipeline не выполняется, пока не вызван env.execute().
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // KafkaSource — новый (рекомендуемый) способ подключения Flink к Kafka
        // (в старых версиях Flink использовался FlinkKafkaConsumer, сейчас deprecated).
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                // group.id для Flink — как и в обычном Kafka consumer, нужен
                // для отслеживания прогресса чтения (хотя Flink хранит offset'ы
                // в первую очередь в своих checkpoint'ах, а не только в Kafka).
                .setGroupId("flink-job-group")
                // EARLIEST — при первом запуске (нет сохранённого checkpoint'а)
                // начинаем читать топик с самого начала, как и в наших предыдущих consumer'ах.
                .setStartingOffsets(OffsetsInitializer.earliest())
                // SimpleStringSchema — Kafka source отдаёт "сырые" сообщения
                // как обычные Java String (то же самое, что делали StringDeserializer
                // в SimpleConsumerApp) — парсинг JSON в EcommerceEvent сделаем сами дальше,
                // отдельным шагом через map().
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Подключаем Kafka source к environment, создавая DataStream<String> —
        // "бесконечный" поток JSON-строк, по одной на каждое сообщение из Kafka.
        // WatermarkStrategy.noWatermarks() — пока не настраиваем watermark здесь,
        // сделаем это явно на следующем шаге, уже после парсинга в EcommerceEvent
        // (watermark должен строиться на основе eventTime из самих данных,
        // а не из сырой JSON-строки).
        DataStream<String> rawEvents = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Парсим каждую JSON-строку в объект EcommerceEvent через MapFunction.
        // MapFunction — простейший тип трансформации в Flink: один вход -> один выход,
        // без сохранения состояния между вызовами (stateless).
        DataStream<EcommerceEvent> events = rawEvents.map(new JsonToEventMapper());

        // Назначаем watermark strategy теперь, когда у нас есть типизированный
        // объект с полем eventTime. forBoundedOutOfOrderness(Duration.ofSeconds(5)) —
        // говорим Flink: "события могут приходить с опозданием до 5 секунд
        // относительно уже увиденного максимального eventTime, подожди это время,
        // прежде чем считать окно окончательно закрытым".
        DataStream<EcommerceEvent> eventsWithWatermarks = events.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        // Указываем Flink, как достать timestamp события из самого объекта —
                        // eventTime в миллисекундах с эпохи, а не System.currentTimeMillis().
                        .withTimestampAssigner((event, timestamp) -> event.eventTime().toEpochMilli())
                        // Если партиция не прислала ни одного нового события за 10 секунд,
                        // считаем её "простаивающей" (idle) и исключаем из расчёта
                        // общего watermark потока. Без этого, если хотя бы одна партиция
                        // пуста или неактивна, весь поток "зависает" — ни одно окно
                        // не закрывается, даже если в других партициях полно готовых данных.
                        .withIdleness(Duration.ofSeconds(10))
        );

        // Группируем поток по типу события (keyBy) — это логическое разделение
        // потока на независимые "подпотоки" по ключу, аналог GROUP BY в SQL,
        // но для потоковых данных. Дальше tumbling-окно будет считаться
        // ОТДЕЛЬНО для каждого значения eventType.
        DataStream<String> result = eventsWithWatermarks
                .keyBy(event -> event.eventType().name())
                // Tumbling-окно длиной 1 минута — фиксированные, непересекающиеся
                // интервалы времени, как договаривались (в противовес Sliding).
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                // ProcessWindowFunction даёт полный доступ к содержимому окна:
                // ключу (eventType), метаданным окна (границы времени) и всем
                // событиям, попавшим в это окно за этот интервал. Мы просто
                // считаем размер коллекции — это и есть наш "count по типу за окно".
                .process(new ProcessWindowFunction<EcommerceEvent, String, String, TimeWindow>() {
                    @Override
                    public void process(
                            String eventType,
                            Context context,
                            Iterable<EcommerceEvent> events,
                            Collector<String> out
                    ) {
                        long count = 0;
                        for (EcommerceEvent ignored : events) {
                            count++;
                        }

                        // context.window() даёт доступ к границам окна (start/end) в виде
                        // миллисекунд с эпохи (epoch millis) — то же представление времени,
                        // что и Instant.toEpochMilli(). Оборачиваем обратно в Instant
                        // и форматируем в читаемую дату-время для вывода.
                        Instant windowStart = Instant.ofEpochMilli(context.window().getStart());
                        Instant windowEnd = Instant.ofEpochMilli(context.window().getEnd());

                        String windowInfo = String.format(
                                "[%s - %s]",
                                FORMATTER.format(windowStart),
                                FORMATTER.format(windowEnd)
                        );

                        out.collect(windowInfo + " " + eventType + ": " + count);
                    }
                });

        // print() — sink, который выводит каждый элемент потока в stdout.
        // Аналог consumer.print() из Kafka, только теперь это встроенный
        // в Flink механизм вывода результата pipeline.
        result.print();

        // execute() запускает весь построенный pipeline. До этого момента
        // ничего реально не происходило — весь код выше только СТРОИЛ граф
        // обработки (это называется "lazy evaluation", как и в Spark).
        env.execute("Event Type Count Job");
    }

    /**
     * Функция преобразования JSON-строки из Kafka в типизированный объект EcommerceEvent.
     * <p>
     * Реализована как отдельный статический класс (а не лямбда), так как
     * ObjectMapper внутри требует инициализации (регистрация JavaTimeModule),
     * и Flink лучше работает с явными классами MapFunction для сериализации
     * между узлами кластера (в проде, при распределённом выполнении) —
     * хотя для локального запуска разница не критична.
     */
    private static class JsonToEventMapper implements MapFunction<String, EcommerceEvent> {

        // transient — поле не должно сериализовываться вместе с самим объектом
        // JsonToEventMapper при распределении задачи по узлам Flink-кластера.
        // ObjectMapper создаётся заново на каждом узле при первом использовании.
        private transient ObjectMapper objectMapper;

        @Override
        public EcommerceEvent map(String json) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
                objectMapper.registerModule(new JavaTimeModule());
            }
            return objectMapper.readValue(json, EcommerceEvent.class);
        }
    }
}