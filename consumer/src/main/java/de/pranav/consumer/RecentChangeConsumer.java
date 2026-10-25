package de.pranav.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class RecentChangeConsumer {

    private static final Logger log = LoggerFactory.getLogger(RecentChangeConsumer.class);

    private final de.pranav.consumer.EventParser parser;
    private final de.pranav.consumer.EventRepository repository;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();

    public RecentChangeConsumer(de.pranav.consumer.EventParser parser, de.pranav.consumer.EventRepository repository) {
        this.parser = parser;
        this.repository = repository;
    }

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        WikiEvent event = parser.parse(record.value());
        int rows = repository.insertIgnoreDuplicate(event, record.partition(), record.offset());

        if (rows == 0) {
            duplicates.incrementAndGet();
        }
        long n = processed.incrementAndGet();
        if (n % 1000 == 0) {
            log.info("Processed {} records, {} duplicates ignored", n, duplicates.get());
        }
    }
}