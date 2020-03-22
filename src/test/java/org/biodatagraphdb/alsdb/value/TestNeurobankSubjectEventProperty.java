package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.NeurobankSubjectEventProperty;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectEventProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectEventProperty.Companion::parseCSVRecord)
            .limit(100)
            .forEach(System.out::println)
        );
  }

}
