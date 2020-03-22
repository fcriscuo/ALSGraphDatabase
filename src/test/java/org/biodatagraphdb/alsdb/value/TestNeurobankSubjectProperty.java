package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.NeurobankSubjectProperty;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectProperty.Companion::parseCSVRecord)
            .limit(100)
            .forEach(System.out::println)
        );
  }

}
