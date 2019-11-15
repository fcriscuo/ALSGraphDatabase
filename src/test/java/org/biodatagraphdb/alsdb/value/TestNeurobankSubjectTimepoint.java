package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectTimepoint {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(org.biodatagraphdb.alsdb.value.NeurobankEventTimepoint::parseCSVRecord)
            .limit(200)
            .forEach(System.out::println)
        );
  }

}
