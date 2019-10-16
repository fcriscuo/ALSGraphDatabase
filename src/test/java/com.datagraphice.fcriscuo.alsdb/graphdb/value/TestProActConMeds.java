package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;

public class TestProActConMeds {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROACT_CONMEDS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.ProActConMeds::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.id() +" " +demo.medication().toLowerCase()))
        );
  }
}
