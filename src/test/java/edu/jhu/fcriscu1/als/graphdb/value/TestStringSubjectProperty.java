package edu.jhu.fcriscu1.als.graphdb.value;

import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

public class TestStringSubjectProperty {

;
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(StringSubjectProperty::parseCSVRecord)
            .limit(50)
            .forEach(System.out::println)
        );


  }

}
