package de.pranav.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public NewTopic recentChangeTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic deadLetterTopic(@Value("${app.dlt-topic}") String dltTopic) {
        return TopicBuilder.name(dltTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                            @Value("${app.dlt-topic}") String dltTopic) {
        // Publish failed records to the DLT; partition -1 = let Kafka choose
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(dltTopic, -1));

        // 5 retries: 0.5s, 1s, 2s, 4s, 8s
        ExponentialBackOffWithMaxRetries backoff = new ExponentialBackOffWithMaxRetries(5);
        backoff.setInitialInterval(500L);
        backoff.setMultiplier(2.0);
        backoff.setMaxInterval(10_000L);

        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}