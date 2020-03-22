package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.ProActDemographics;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;

public class TestProActDemographics {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_DEMOGRAPHICS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActDemographics.Companion::parseCSVRecord)
            .limit(400)
            .forEach(demo -> System.out.println(demo.getSubjectGuid()))
        );
  }
}
