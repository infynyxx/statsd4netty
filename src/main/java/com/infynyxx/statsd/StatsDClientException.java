package com.infynyxx.statsd;

public class StatsDClientException extends Exception {
    public StatsDClientException(Exception e) {
        super(e);
    }

    public StatsDClientException(final String message) {
        super(message);
    }
}
