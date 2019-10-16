package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;

public class TestProActDemographics {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_DEMOGRAPHICS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.ProActDemographics::parseCSVRecord)
            .limit(4000)
            .forEach(demo -> System.out.println(demo.subjectGuid()))
        );
  }
}
