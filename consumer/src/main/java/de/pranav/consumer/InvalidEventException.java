package de.pranav.consumer;

public class InvalidEventException extends RuntimeException {

    private final String reason;

    public InvalidEventException(String reason, String detail) {
        super(reason + ": " + detail);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}