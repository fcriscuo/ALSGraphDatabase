package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;

public class TestProActDemographics {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_DEMOGRAPHICS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActDemographics::parseCSVRecord)
            .limit(4000)
            .forEach(demo -> System.out.println(demo.subjectGuid()))
        );
  }
}
