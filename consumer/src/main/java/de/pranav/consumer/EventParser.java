package de.pranav.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.pranav.consumer.WikiEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class EventParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public WikiEvent parse(String json) throws JsonProcessingException {
        JsonNode n = mapper.readTree(json);
        return new WikiEvent(
                UUID.fromString(n.at("/meta/id").asText()),
                Instant.parse(n.at("/meta/dt").asText()),
                n.path("wiki").asText(),
                n.path("type").asText(),
                intOrNull(n.path("namespace")),
                n.path("title").asText(null),
                n.path("user").asText(null),
                n.path("bot").asBoolean(false),
                n.has("minor") ? n.path("minor").asBoolean() : null,
                intOrNull(n.at("/length/old")),
                intOrNull(n.at("/length/new")));
    }

    private static Integer intOrNull(JsonNode node) {
        return node.isMissingNode() || node.isNull() ? null : node.asInt();
    }
}