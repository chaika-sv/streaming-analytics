package ru.chaika.streaming.producer;

import ru.chaika.streaming.producer.parser.CsvEventParser;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestParserRun {

    public static void main(String[] args) throws IOException {
        String filePath = "producer/src/main/resources/data/sample.csv";
        AtomicInteger count = new AtomicInteger(0);

        CsvEventParser.parse(filePath, event -> {
            if (count.get() < 10) {
                System.out.println(event);
            }
            count.incrementAndGet();
        });

        System.out.println("Всего событий обработано: " + count.get());
    }
}