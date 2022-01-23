package com.usegutierrez.exception;

public class ESConsumerException extends RuntimeException {

  public ESConsumerException() {
  }

  public ESConsumerException(String message) {
    super(message);
  }

  public ESConsumerException(String message, Throwable cause) {
    super(message, cause);
  }

  public ESConsumerException(Throwable cause) {
    super(cause);
  }

  public ESConsumerException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
