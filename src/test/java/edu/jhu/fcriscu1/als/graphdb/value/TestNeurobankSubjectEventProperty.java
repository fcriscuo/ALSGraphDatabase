package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectEventProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectEventProperty::parseCSVRecord)
            .limit(2000)
            .forEach(System.out::println)
        );
  }

}
