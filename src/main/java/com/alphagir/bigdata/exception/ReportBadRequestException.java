package com.alphagir.bigdata.exception;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReportBadRequestException extends RuntimeException {
    public ReportBadRequestException(String message) {
        super(message);
        log.error(message);
    }

    public ReportBadRequestException(String message, Throwable cause) {
        super(message, cause);
        log.error(message, cause);
    }
}
