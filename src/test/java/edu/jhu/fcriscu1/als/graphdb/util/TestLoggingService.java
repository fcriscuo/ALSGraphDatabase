package edu.jhu.fcriscu1.als.graphdb.util;

import java.util.stream.IntStream;

public class TestLoggingService {

  public static void main(String[] args) {
    IntStream.rangeClosed(1,50).forEach((i) -> {
      // info msg
      AsyncLoggingService.logInfo("This is  info message: " +i);
      // debug message
      AsyncLoggingService.logDebug("This is  debug message: " +i);
      // error message
      AsyncLoggingService.logError("This is  error message: " +i);
    });

    AsyncLoggingService.close();
  }

}
