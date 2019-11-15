package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectEventProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(org.biodatagraphdb.alsdb.value.NeurobankSubjectEventProperty::parseCSVRecord)
            .limit(2000)
            .forEach(System.out::println)
        );
  }

}
