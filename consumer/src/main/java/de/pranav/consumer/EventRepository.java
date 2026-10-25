package de.pranav.consumer;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Repository
public class EventRepository {

    private static final String INSERT_SQL = """
            INSERT INTO wiki_events
              (event_id, event_time, wiki, event_type, namespace, title, username,
               is_bot, is_minor, length_old, length_new, kafka_partition, kafka_offset)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private final JdbcTemplate jdbc;

    public EventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @return 1 if inserted, 0 if the event already existed (duplicate) */
    public int insertIgnoreDuplicate(de.pranav.consumer.WikiEvent e, int partition, long offset) {
        return jdbc.update(INSERT_SQL,
                e.eventId(), Timestamp.from(e.eventTime()), e.wiki(), e.eventType(),
                e.namespace(), e.title(), e.username(), e.bot(), e.minor(),
                e.lengthOld(), e.lengthNew(), partition, offset);
    }
}