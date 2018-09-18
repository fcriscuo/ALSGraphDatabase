package org.nygenome.als.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;

public class TestStringSubjectProperty {
  static Logger log = Logger.get(TestStringSubjectProperty.class);
  //TODO: make property
  static final String DEFAULT_FILE = "/data/als/subject_property.csv"
;
  public static void main(String[] args) {
    Path filePath = (args.length>0) ? Paths.get(args[0])
        : Paths.get(DEFAULT_FILE);
    if(Files.exists(filePath,LinkOption.NOFOLLOW_LINKS)) {
      new CsvRecordStreamSupplier(filePath)
          .get()
          .map(StringSubjectProperty::parseCSVRecord)
          .limit(200)
          .forEach(System.out::println);
    }

  }

}
