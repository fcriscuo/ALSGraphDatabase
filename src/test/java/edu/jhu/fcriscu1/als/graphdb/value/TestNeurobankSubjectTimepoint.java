package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

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
