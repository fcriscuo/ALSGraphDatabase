package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.ProActConMeds;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActConMeds {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROACT_CONMEDS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActConMeds.Companion::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.getId() +" " +demo.getMedication().toLowerCase()))
        );
  }
}
