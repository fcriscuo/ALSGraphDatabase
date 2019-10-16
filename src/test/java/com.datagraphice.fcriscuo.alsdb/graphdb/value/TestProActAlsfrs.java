package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;

public class TestProActAlsfrs {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROCACT_ALSFRS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.ProActAlsfrs::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.id()
                    +"  " +demo.alsfrsDelta()
                    +" " +demo.alsTotal()
                     +"  " +demo.alsfrsrTotal()))
        );
  }
}
