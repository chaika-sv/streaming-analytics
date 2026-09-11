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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.chaika.streaming.common.model.EcommerceEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Flink job, считающий топ-5 категорий товаров по общему числу событий
 * (view + cart + purchase вместе) за каждое минутное окно.
 * <p>
 * В отличие от EventTypeCountJob, здесь НЕ используется keyBy() перед окном —
 * это намеренное архитектурное решение. keyBy() разбил бы поток на независимые
 * "дорожки" по ключу (например, по categoryCode), и каждая дорожка считала бы
 * свою метрику изолированно, не "видя" другие категории. А нам как раз нужно
 * сравнивать категории МЕЖДУ СОБОЙ внутри одного окна, чтобы выбрать топ-5 —
 * то есть вся агрегация должна происходить в одном месте, с доступом
 * ко всем событиям окна сразу.
 */
public class CategoryTopNJob {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ecommerce-events";
    private static final int TOP_N = 5;

    // Категория, которую присваиваем событиям с пустым categoryCode —
    // напомним, в EcommerceEvent это поле nullable (не все товары
    // в датасете имеют заполненную категорию).
    private static final String UNKNOWN_CATEGORY = "unknown";

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                // Отдельный group.id от EventTypeCountJob — иначе оба job'а,
                // запущенные одновременно, будут делить партиции топика между собой
                // (как обычный Kafka consumer rebalancing), а не читать независимо.
                .setGroupId("category-topn-job-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        DataStream<EcommerceEvent> events = rawEvents.map(new JsonToEventMapper());

        DataStream<EcommerceEvent> eventsWithWatermarks = events.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<EcommerceEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.eventTime().toEpochMilli())
                        .withIdleness(Duration.ofSeconds(10))
        );

        // windowAll() — глобальное окно без предварительного keyBy().
        // Важное следствие: вся агрегация внутри такого окна выполняется
        // НА ОДНОМ parallel subtask'е (параллелизм здесь фактически равен 1),
        // так как нет ключа, по которому можно было бы разделить нагрузку.
        // Для пет-проекта с небольшим потоком это некритично, но в проде
        // с высоким throughput это потенциальное узкое место — стоит помнить.
        DataStream<String> topCategories = eventsWithWatermarks
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TopNCategoriesFunction());

        topCategories.print();

        env.execute("Category Top-N Job");
    }

    /**
     * Считает количество событий по каждой категории внутри окна,
     * сортирует по убыванию и выводит топ-N в виде многострочной строки.
     * <p>
     * ProcessAllWindowFunction (в отличие от ProcessWindowFunction,
     * который мы использовали в EventTypeCountJob) применяется именно
     * к глобальному окну без ключа — отсюда и "All" в названии.
     */
    private static class TopNCategoriesFunction
            extends ProcessAllWindowFunction<EcommerceEvent, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<EcommerceEvent> events, Collector<String> out) {

            // Считаем количество событий по каждой категории вручную через Map —
            // по сути, ручная реализация GROUP BY categoryCode, COUNT(*)
            // прямо здесь, внутри Java-кода, так как весь набор событий окна
            // уже находится у нас в памяти на этом этапе (Iterable<EcommerceEvent>).
            Map<String, Long> countsByCategory = new HashMap<>();

            for (EcommerceEvent event : events) {
                // categoryCode может быть null (не все товары имеют категорию) —
                // группируем такие события под отдельный ключ "unknown",
                // а не пропускаем их и не даём упасть на NullPointerException
                // при использовании null как ключа HashMap.
                String category = event.categoryCode() != null ? event.categoryCode() : UNKNOWN_CATEGORY;

                countsByCategory.merge(category, 1L, Long::sum);
            }

            // Сортируем категории по убыванию количества событий и берём первые TOP_N.
            // Stream API тут удобнее, чем ручная сортировка списка —
            // Map.Entry.comparingByValue() сравнивает записи именно по value (count),
            // reversed() даёт сортировку по убыванию (сначала самые популярные).
            List<Map.Entry<String, Long>> topEntries = countsByCategory.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(TOP_N)
                    .collect(Collectors.toList());

            Instant windowStart = Instant.ofEpochMilli(context.window().getStart());
            Instant windowEnd = Instant.ofEpochMilli(context.window().getEnd());

            StringBuilder result = new StringBuilder();
            result.append(String.format(
                    "[%s - %s] Top-%d categories:%n",
                    FORMATTER.format(windowStart), FORMATTER.format(windowEnd), TOP_N
            ));

            // rank начинается с 1, а не с 0 — для человекочитаемого вывода
            // ("1. категория: количество", а не "0. категория: количество").
            int rank = 1;
            for (Map.Entry<String, Long> entry : topEntries) {
                result.append(String.format("  %d. %s: %d%n", rank, entry.getKey(), entry.getValue()));
                rank++;
            }

            out.collect(result.toString());
        }
    }

    /**
     * Идентичен JsonToEventMapper из EventTypeCountJob — дублируем здесь,
     * а не выносим в common, так как это внутренняя деталь конкретного job'а,
     * а не переиспользуемая бизнес-логика (как EcommerceEvent или KafkaEventProducer).
     */
    private static class JsonToEventMapper implements MapFunction<String, EcommerceEvent> {

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