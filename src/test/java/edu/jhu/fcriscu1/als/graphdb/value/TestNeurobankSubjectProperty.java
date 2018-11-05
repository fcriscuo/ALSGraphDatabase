package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectProperty::parseCSVRecord)
            .limit(200)
            .forEach(System.out::println)
        );
  }

}
