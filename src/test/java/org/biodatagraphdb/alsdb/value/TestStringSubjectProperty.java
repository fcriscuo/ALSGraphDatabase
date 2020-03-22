package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.StringSubjectProperty;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestStringSubjectProperty {

;
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(StringSubjectProperty.Companion::parseCSVRecord)
            .limit(50)
            .forEach(System.out::println)
        );


  }

}
