package de.pranav.consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventParserTest {

    private final EventParser parser = new EventParser();

    private static final String VALID = """
            {"meta":{"id":"eec6bc06-8e88-4d38-913f-4a1c26eb3743","dt":"2026-10-24T10:58:01.179Z"},
             "type":"edit","namespace":2,"title":"Test","user":"Jirayupp","bot":false,"minor":false,
             "length":{"old":2952,"new":3439},"wiki":"thwiki"}
            """;

    @Test
    void parsesValidEvent() {
        WikiEvent e = parser.parse(VALID);

        assertThat(e.eventId()).hasToString("eec6bc06-8e88-4d38-913f-4a1c26eb3743");
        assertThat(e.wiki()).isEqualTo("thwiki");
        assertThat(e.lengthNew()).isEqualTo(3439);
        assertThat(e.bot()).isFalse();
    }

    @Test
    void allowsMissingLengthLikeCategorizeEvents() {
        String json = """
                {"meta":{"id":"c7bfc6c9-d2b6-459d-9610-683c7d0e3bf9","dt":"2026-10-24T12:11:19.461Z"},
                 "type":"categorize","namespace":14,"user":"Hamish-bot","bot":true,"wiki":"zhwiki"}
                """;

        WikiEvent e = parser.parse(json);

        assertThat(e.lengthOld()).isNull();
        assertThat(e.lengthNew()).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badInputs")
    void rejectsInvalidInput(String expectedReason, String json) {
        assertThatThrownBy(() -> parser.parse(json))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageStartingWith(expectedReason + ":");
    }

    static Stream<Arguments> badInputs() {
        return Stream.of(
                Arguments.of("NULL_VALUE", null),
                Arguments.of("MALFORMED_JSON", "not json"),
                Arguments.of("NOT_AN_OBJECT", "[1,2,3]"),
                Arguments.of("MISSING_FIELD",
                        "{\"wiki\":\"x\",\"type\":\"edit\",\"meta\":{\"dt\":\"2026-10-24T10:58:01Z\"}}"),
                Arguments.of("INVALID_UUID",
                        "{\"meta\":{\"id\":\"abc\",\"dt\":\"2026-10-24T10:58:01Z\"},\"wiki\":\"x\",\"type\":\"edit\"}"),
                Arguments.of("INVALID_TIMESTAMP",
                        "{\"meta\":{\"id\":\"eec6bc06-8e88-4d38-913f-4a1c26eb3743\",\"dt\":\"yesterday\"},\"wiki\":\"x\",\"type\":\"edit\"}")
        );
    }
}