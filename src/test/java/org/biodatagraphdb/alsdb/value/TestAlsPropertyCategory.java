package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestAlsPropertyCategory {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALS_PROPERTY_CATEGORY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(org.biodatagraphdb.alsdb.value.AlsPropertyCategory::parseCSVRecord)
            //.limit(50)
            .forEach(System.out::println)
        );
  }
}
