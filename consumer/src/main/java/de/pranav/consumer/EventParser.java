package de.pranav.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.pranav.consumer.WikiEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.UUID;

@Component
public class EventParser {

    private final ObjectMapper mapper = new ObjectMapper();

    public WikiEvent parse(String json) {
        if (json == null) {
            throw new InvalidEventException("NULL_VALUE", "record has no value");
        }
        JsonNode n;
        try {
            n = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("MALFORMED_JSON", e.getOriginalMessage());
        }
        if (!n.isObject()) {
            throw new InvalidEventException("NOT_AN_OBJECT", "top-level JSON is not an object");
        }

        return new WikiEvent(
                parseUuid(n.at("/meta/id")),
                parseInstant(n.at("/meta/dt")),
                requiredText(n, "wiki"),
                requiredText(n, "type"),
                intOrNull(n.path("namespace")),
                textOrNull(n.path("title")),
                textOrNull(n.path("user")),
                n.path("bot").asBoolean(false),
                n.hasNonNull("minor") ? n.path("minor").asBoolean() : null,
                intOrNull(n.at("/length/old")),
                intOrNull(n.at("/length/new")));
    }

    private static UUID parseUuid(JsonNode node) {
        if (!node.isTextual()) {
            throw new InvalidEventException("MISSING_FIELD", "meta.id");
        }
        try {
            return UUID.fromString(node.asText());
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("INVALID_UUID", node.asText());
        }
    }

    private static Instant parseInstant(JsonNode node) {
        if (!node.isTextual()) {
            throw new InvalidEventException("MISSING_FIELD", "meta.dt");
        }
        try {
            return Instant.parse(node.asText());
        } catch (DateTimeParseException e) {
            throw new InvalidEventException("INVALID_TIMESTAMP", node.asText());
        }
    }

    private static String requiredText(JsonNode n, String field) {
        JsonNode v = n.path(field);
        if (!v.isTextual() || v.asText().isBlank()) {
            throw new InvalidEventException("MISSING_FIELD", field);
        }
        return v.asText();
    }

    private static String textOrNull(JsonNode v) {
        return v.isTextual() ? v.asText() : null;
    }

    private static Integer intOrNull(JsonNode v) {
        return v.isNumber() ? v.asInt() : null;
    }
}