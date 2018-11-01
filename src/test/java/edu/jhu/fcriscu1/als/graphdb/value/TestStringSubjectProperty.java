package org.nygenome.als.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

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
