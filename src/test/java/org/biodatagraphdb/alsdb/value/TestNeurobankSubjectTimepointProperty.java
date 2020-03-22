package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.NeurobankSubjectTimepointProperty;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectTimepointProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(NeurobankSubjectTimepointProperty.Companion::parseCSVRecord)
            .limit(100)
            .forEach(System.out::println)
        );
  }

}
