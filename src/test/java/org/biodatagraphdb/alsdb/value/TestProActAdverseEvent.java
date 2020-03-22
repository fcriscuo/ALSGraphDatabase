package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.ProActAdverseEvent;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActAdverseEvent {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActAdverseEvent.Companion::parseCSVRecord)
            .limit(400)
            .map(event -> event.getId())
            .forEach(System.out::println)
        );
  }


}
