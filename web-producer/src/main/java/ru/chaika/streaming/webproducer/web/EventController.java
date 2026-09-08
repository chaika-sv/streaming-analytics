package ru.chaika.streaming.webproducer.web;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.chaika.streaming.common.kafka.KafkaEventProducer;
import ru.chaika.streaming.common.model.EcommerceEvent;
import ru.chaika.streaming.common.model.EventType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * REST-контроллер, принимающий данные из веб-формы и отправляющий
 * на их основе EcommerceEvent в Kafka-топик manual-events.
 */
@RestController
public class EventController {

    private final KafkaEventProducer kafkaEventProducer;

    public EventController(KafkaEventProducer kafkaEventProducer) {
        this.kafkaEventProducer = kafkaEventProducer;
    }

    @PostMapping("/api/events")
    public String sendEvent(@RequestBody ManualEventRequest request) {
        EcommerceEvent event = new EcommerceEvent(
                // eventTime не запрашиваем у пользователя — берём момент отправки,
                // это ближе к реальному сценарию "события прямо сейчас"
                Instant.now(),
                EventType.fromRawValue(request.eventType()),
                Long.parseLong(request.productId()),
                Long.parseLong(request.categoryId()),
                // Пустая строка из формы — это "поле не заполнено", приводим к null,
                // как и договаривались для EcommerceEvent.categoryCode/brand
                request.categoryCode().isBlank() ? null : request.categoryCode(),
                request.brand().isBlank() ? null : request.brand(),
                new BigDecimal(request.price()),
                Long.parseLong(request.userId()),
                // userSession необязательное поле в форме — если пусто, генерируем
                // случайный UUID сами, чтобы объект EcommerceEvent был валиден
                request.userSession().isBlank() ? UUID.randomUUID().toString() : request.userSession()
        );

        kafkaEventProducer.send(event);
        return "Event sent: " + event;
    }
}