package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.ProActAlsfrs;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActAlsfrs {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROCACT_ALSFRS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActAlsfrs.Companion::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.getId()
                    +"  " +demo.getAlsfrsDelta()
                    +" " +demo.getAlsfrsPartialTotal()
                     +"  " +demo.getAlsfrsrTotal()))
        );
  }
}
