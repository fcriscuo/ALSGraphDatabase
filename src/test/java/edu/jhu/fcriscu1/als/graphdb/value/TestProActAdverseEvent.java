package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.CsvRecordStreamSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;

public class TestProActAdverseEvent {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActAdverseEvent::parseCSVRecord)
            .limit(4000)
            .map(ProActAdverseEvent::id)
            .forEach(System.out::println)
        );
  }


}
