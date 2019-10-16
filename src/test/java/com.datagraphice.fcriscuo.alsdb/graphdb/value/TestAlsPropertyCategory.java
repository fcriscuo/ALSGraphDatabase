package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestAlsPropertyCategory {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALS_PROPERTY_CATEGORY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(edu.jhu.fcriscu1.als.graphdb.value.AlsPropertyCategory::parseCSVRecord)
            //.limit(50)
            .forEach(System.out::println)
        );
  }
}
