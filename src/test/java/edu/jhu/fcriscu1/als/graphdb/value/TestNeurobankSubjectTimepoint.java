package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectTimepoint {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankEventTimepoint::parseCSVRecord)
            .limit(200)
            .forEach(System.out::println)
        );
  }

}
