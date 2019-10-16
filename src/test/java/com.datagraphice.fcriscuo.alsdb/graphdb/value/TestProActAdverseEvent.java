package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;

public class TestProActAdverseEvent {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.ProActAdverseEvent::parseCSVRecord)
            .limit(4000)
            .map(edu.jhu.fcriscu1.als.graphdb.value.ProActAdverseEvent::id)
            .forEach(System.out::println)
        );
  }


}
