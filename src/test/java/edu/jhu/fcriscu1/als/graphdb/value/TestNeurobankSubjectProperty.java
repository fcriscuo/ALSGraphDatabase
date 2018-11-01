package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

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
