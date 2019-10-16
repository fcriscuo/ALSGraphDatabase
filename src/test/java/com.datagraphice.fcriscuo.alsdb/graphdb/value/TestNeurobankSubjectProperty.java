package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestNeurobankSubjectProperty {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("NEUROBANK_SUBJECT_PROPERTY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.NeurobankSubjectProperty::parseCSVRecord)
            .limit(200)
            .forEach(System.out::println)
        );
  }

}
