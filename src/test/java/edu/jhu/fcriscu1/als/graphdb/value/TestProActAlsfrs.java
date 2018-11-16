package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.CsvRecordStreamSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;

public class TestProActAlsfrs {
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROCACT_ALSFRS_FILE")
        .ifPresent((path) -> new CsvRecordStreamSupplier(path)
            .get()
            .map(ProActAlsfrs::parseCSVRecord)
            .forEach(demo -> System.out.println(demo.id()
                    +"  " +demo.alsfrsDelta()
                    +" " +demo.alsTotal()
                     +"  " +demo.alsfrsrTotal()))
        );
  }
}
