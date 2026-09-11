package ru.chaika.streaming.flinkjob;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.chaika.streaming.common.model.EcommerceEvent;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Flink job: читает события из Kafka, группирует по типу события
 * (view/cart/purchase) внутри тумблинг-окна в 1 минуту, считает количество
 * событий каждого типа и пишет результат и в консоль (для наглядности
 * во время разработки), и в ClickHouse (для персистентного хранения).
 */
public class EventTypeCountJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ecommerce-events";

    // Параметры подключения к ClickHouse. bootstrap-servers Kafka мы тоже
    // хардкодим прямо в коде (не вынося в конфиг) — для пет-проекта
    // это осознанное упрощение, в реальном приложении такие вещи
    // обычно выносят во внешний конфиг/переменные окружения.
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/streaming_analytics";
    private static final String CLICKHOUSE_USER = "default";
    private static final String CLICKHOUSE_PASSWORD = "clickhouse";

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
        //
        // process() теперь возвращает не готовую строку для печати,
        // а структурированный объект EventTypeCountResult — это даёт
        // сразу два независимых потребителя результата: консоль (для
        // визуальной проверки по ходу разработки) и ClickHouse (для
        // персистентного хранения), не дублируя логику подсчёта.
        DataStream<EventTypeCountResult> result = eventsWithWatermarks
                .keyBy(event -> event.eventType().name())
                // Tumbling-окно длиной 1 минута — фиксированные, непересекающиеся
                // интервалы времени, как договаривались (в противовес Sliding).
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<EcommerceEvent, EventTypeCountResult, String, TimeWindow>() {
                    @Override
                    public void process(
                            String eventType,
                            Context context,
                            Iterable<EcommerceEvent> events,
                            Collector<EventTypeCountResult> out
                    ) {
                        long count = 0;
                        for (EcommerceEvent ignored : events) {
                            count++;
                        }

                        // context.window() даёт доступ к границам окна (start/end) в виде
                        // миллисекунд с эпохи (epoch millis) — то же представление времени,
                        // что и Instant.toEpochMilli().
                        out.collect(new EventTypeCountResult(
                                context.window().getStart(),
                                context.window().getEnd(),
                                eventType,
                                count
                        ));
                    }
                });

        // Sink 1: печать в консоль — оставляем для наглядности во время разработки,
        // так проще сразу видеть, что происходит, не заглядывая каждый раз в ClickHouse.
        result.map(r -> String.format(
                "[%s - %s] %s: %d",
                FORMATTER.format(Instant.ofEpochMilli(r.windowStart())),
                FORMATTER.format(Instant.ofEpochMilli(r.windowEnd())),
                r.eventType(),
                r.count()
        )).print();

        // Sink 2: запись в ClickHouse через JDBC.
        result.addSink(JdbcSink.sink(
                // SQL-шаблон с плейсхолдерами — как обычный PreparedStatement,
                // который ты наверняка писал через MyBatis/JDBC на работе.
                "INSERT INTO event_type_counts (window_start, window_end, event_type, event_count) VALUES (?, ?, ?, ?)",
                // JdbcStatementBuilder — говорит Flink, как заполнить плейсхолдеры
                // из конкретного объекта EventTypeCountResult. Вызывается для
                // каждой записи перед её отправкой в batch.
                (JdbcStatementBuilder<EventTypeCountResult>) (statement, r) -> {
                    // ClickHouse JDBC-драйвер ожидает java.sql.Timestamp для DateTime-колонок,
                    // а не java.time.Instant напрямую — оборачиваем через Timestamp.from().
                    statement.setTimestamp(1, Timestamp.from(Instant.ofEpochMilli(r.windowStart())));
                    statement.setTimestamp(2, Timestamp.from(Instant.ofEpochMilli(r.windowEnd())));
                    statement.setString(3, r.eventType());
                    statement.setLong(4, r.count());
                },
                // Настройки батчинга — Flink не шлёт INSERT на каждую отдельную
                // запись (это было бы очень медленно), а копит их в батч
                // и отправляет либо когда накопилось batchSize записей,
                // либо когда прошло batchIntervalMs миллисекунд — что наступит раньше.
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(5000)
                        .withMaxRetries(3)
                        .build(),
                // Параметры подключения к ClickHouse.
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(CLICKHOUSE_URL)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(CLICKHOUSE_USER)
                        .withPassword(CLICKHOUSE_PASSWORD)
                        .build()
        ));

        // execute() запускает весь построенный pipeline. До этого момента
        // ничего реально не происходило — весь код выше только СТРОИЛ граф
        // обработки (это называется "lazy evaluation", как и в Spark).
        env.execute("Event Type Count Job");
    }

    /**
     * Промежуточный результат оконной агрегации — количество событий
     * определённого типа за конкретное временное окно.
     * <p>
     * Вынесен в отдельный record, а не просто оставлен строкой,
     * чтобы этот же объект можно было передать сразу в два разных sink'а
     * (консоль и ClickHouse) без дублирования логики подсчёта.
     */
    public record EventTypeCountResult(
            long windowStart,
            long windowEnd,
            String eventType,
            long count
    ) {
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