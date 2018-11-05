package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectTimepointProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectTimepointProperty::parseCSVRecord)
            .limit(2000)
            .forEach(System.out::println)
        );
  }

}
