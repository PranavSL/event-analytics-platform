package de.pranav.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class WikimediaStreamReader {

    private static final Logger log = LoggerFactory.getLogger(WikimediaStreamReader.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();

    private final String topic;
    private final String streamUrl;
    private final String userAgent;

    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile boolean running = true;

    public WikimediaStreamReader(KafkaTemplate<String, String> kafka,
                                 @Value("${app.topic}") String topic,
                                 @Value("${app.stream-url}") String streamUrl,
                                 @Value("${app.user-agent}") String userAgent) {
        this.kafka = kafka;
        this.topic = topic;
        this.streamUrl = streamUrl;
        this.userAgent = userAgent;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread.ofVirtual().name("sse-reader").start(this::loop);
    }

    private void loop() {
        long backoffMs = 1_000;
        while (running) {
            try {
                streamOnce();
                backoffMs = 1_000;               // connection ended normally, reset
            } catch (Exception e) {
                log.warn("Stream error: {}. Reconnecting in {} ms", e.getMessage(), backoffMs);
            }
            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            backoffMs = Math.min(backoffMs * 2, 30_000);   // exponential backoff, max 30 s
        }
    }

    private void streamOnce() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(streamUrl))
                .header("User-Agent", userAgent)
                .header("Accept", "text/event-stream")
                .build();

        HttpResponse<java.util.stream.Stream<String>> response =
                http.send(request, HttpResponse.BodyHandlers.ofLines());

        if (response.statusCode() != 200) {
            throw new IllegalStateException("HTTP " + response.statusCode());
        }
        log.info("Connected to {}", streamUrl);

        try (var lines = response.body()) {
            lines.forEach(this::handleLine);
        }
    }

    private void handleLine(String line) {
        if (!line.startsWith("data: ")) {
            return;                                  // skip event:, id:, comments, blank lines
        }
        String json = line.substring(6);
        try {
            JsonNode node = mapper.readTree(json);
            String wiki = node.path("wiki").asText(null);   // partition key

            kafka.send(topic, wiki, json).whenComplete((result, ex) -> {
                if (ex != null) {
                    failed.incrementAndGet();
                    log.error("Send failed: {}", ex.getMessage());
                }
            });

            long n = sent.incrementAndGet();
            if (n % 1000 == 0) {
                log.info("Sent {} events ({} failed)", n, failed.get());
            }
        } catch (JsonProcessingException e) {
            log.warn("Skipping unparseable line: {}", e.getMessage());
        }
    }
}