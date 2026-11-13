package de.pranav.producer;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class WikimediaStreamReaderTest {

    private final KafkaTemplate<String, String> kafka = mockTemplate();
    private final WikimediaStreamReader reader =
            new WikimediaStreamReader(kafka, "test-topic", "http://unused", "test-agent");

    @Test
    void forwardsDataLinesKeyedByWiki() {
        String json = "{\"wiki\":\"enwiki\",\"type\":\"edit\"}";

        reader.handleLine("data: " + json);

        verify(kafka).send("test-topic", "enwiki", json);
    }

    @Test
    void ignoresNonDataLines() {
        reader.handleLine("event: message");
        reader.handleLine(": heartbeat");
        reader.handleLine("");

        verifyNoInteractions(kafka);
    }

    @Test
    void skipsUnparseableJson() {
        reader.handleLine("data: {broken");

        verifyNoInteractions(kafka);
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> mockTemplate() {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        return template;
    }
}