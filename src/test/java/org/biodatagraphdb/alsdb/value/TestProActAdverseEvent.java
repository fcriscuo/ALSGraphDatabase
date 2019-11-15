package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActAdverseEvent {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(org.biodatagraphdb.alsdb.value.ProActAdverseEvent::parseCSVRecord)
            .limit(4000)
            .map(org.biodatagraphdb.alsdb.value.ProActAdverseEvent::id)
            .forEach(System.out::println)
        );
  }


}
