package de.pranav.consumer;

import java.time.Instant;
import java.util.UUID;

public record WikiEvent(
        UUID eventId,
        Instant eventTime,
        String wiki,
        String eventType,
        Integer namespace,
        String title,
        String username,
        boolean bot,
        Boolean minor,
        Integer lengthOld,
        Integer lengthNew) {
}