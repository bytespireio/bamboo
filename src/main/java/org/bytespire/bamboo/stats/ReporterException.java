package org.bytespire.bamboo.stats;

/** thrown on failure to report stats. */
public class ReporterException extends Exception {
  public ReporterException(String message) {
    super(message);
  }

  public ReporterException(String message, Throwable cause) {
    super(message, cause);
  }
}
