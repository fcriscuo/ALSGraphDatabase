package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActAlsfrs {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROCACT_ALSFRS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(org.biodatagraphdb.alsdb.value.ProActAlsfrs::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.id()
                    +"  " +demo.alsfrsDelta()
                    +" " +demo.alsTotal()
                     +"  " +demo.alsfrsrTotal()))
        );
  }
}
