package de.pranav.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class ConsumerIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Value("${app.topic}")
    String topic;

    @Value("${app.dlt-topic}")
    String dltTopic;

    @Test
    void validEventIsStoredInPostgres() {
        UUID id = UUID.randomUUID();

        kafkaTemplate.send(topic, "testwiki", eventJson(id));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(countEvents(id)).isEqualTo(1));
    }

    @Test
    void replayedEventIsStoredOnlyOnce() {
        UUID duplicated = UUID.randomUUID();
        UUID marker = UUID.randomUUID();

        // Same key -> same partition -> the consumer handles them in this order
        kafkaTemplate.send(topic, "testwiki", eventJson(duplicated));
        kafkaTemplate.send(topic, "testwiki", eventJson(duplicated));
        kafkaTemplate.send(topic, "testwiki", eventJson(marker));

        // Once the marker is stored, both copies before it have been processed
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(countEvents(marker)).isEqualTo(1));
        assertThat(countEvents(duplicated)).isEqualTo(1);
    }

    @Test
    void malformedRecordGoesToDeadLetterTopic() {
        String poison = "not-json-" + UUID.randomUUID();

        kafkaTemplate.send(topic, "testwiki", poison);

        assertThat(dltContains(poison)).isTrue();
    }

    @Test
    void eventWithoutIdGoesToDeadLetterTopicAndIsNotStored() {
        String marker = "missing-id-" + UUID.randomUUID();
        String json = """
                {"meta":{"dt":"2026-10-24T10:58:01.179Z"},"type":"edit","wiki":"testwiki","title":"%s"}
                """.formatted(marker);

        kafkaTemplate.send(topic, "testwiki", json);

        assertThat(dltContains(marker)).isTrue();
        Integer stored = jdbc.queryForObject(
                "SELECT count(*) FROM wiki_events WHERE title = ?", Integer.class, marker);
        assertThat(stored).isZero();
    }

    private Integer countEvents(UUID id) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM wiki_events WHERE event_id = ?", Integer.class, id);
    }

    private boolean dltContains(String needle) {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "test-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(dltTopic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.value() != null && record.value().contains(needle)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static String eventJson(UUID id) {
        return """
                {"meta":{"id":"%s","dt":"2026-10-24T10:58:01.179Z"},"type":"edit","namespace":0,
                 "title":"Test","user":"tester","bot":false,"minor":false,
                 "length":{"old":10,"new":20},"wiki":"testwiki"}
                """.formatted(id);
    }
}