package ru.chaika.streaming.springconsumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.chaika.streaming.common.model.EcommerceEvent;

/**
 * Слушатель топика ecommerce-events на базе Spring Kafka.
 * <p>
 * Интересно сравнить с SimpleConsumerApp: там мы вручную писали цикл while(true) { poll(); ...; commitSync(); }.
 * Здесь всю эту механику Spring делает сам — достаточно объявить метод
 * с аннотацией @KafkaListener, и фреймворк будет вызывать его для каждого
 * входящего сообщения, используя ConsumerFactory, настроенную в KafkaConsumerConfig.
 */
@Component
public class EcommerceEventListener {

    /**
     * Обрабатывает одно событие из топика ecommerce-events.
     *
     * @param record        полная запись Kafka (даёт доступ к partition/offset,
     *                      как и в SimpleConsumerApp — просто через другой API)
     * @param acknowledgment объект для явного подтверждения обработки.
     *                      Вызов acknowledge() — прямой аналог consumer.commitSync()
     *                      из голого Kafka Consumer API.
     */
    @KafkaListener(topics = "ecommerce-events", groupId = "spring-consumer-group")
    public void onEvent(ConsumerRecord<String, EcommerceEvent> record, Acknowledgment acknowledgment) {
        EcommerceEvent event = record.value();

        System.out.printf(
                "partition=%d offset=%d key=%s event=%s%n",
                record.partition(), record.offset(), record.key(), event
        );

        // Подтверждаем обработку явно — без этого вызова, при ack-mode=manual_immediate,
        // offset вообще не сдвинется, и при перезапуске приложение прочитает
        // это же сообщение снова.
        acknowledgment.acknowledge();
    }
}