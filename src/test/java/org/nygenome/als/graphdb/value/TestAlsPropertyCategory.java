package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestAlsPropertyCategory {

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALS_PROPERTY_CATEGORY_FILE")
        .ifPresent((path) -> new TsvRecordStreamSupplier(path)
            .get()
            .map(AlsPropertyCategory::parseCSVRecord)
            //.limit(50)
            .forEach(System.out::println)
        );
  }
}
