package org.biodatagraphdb.alsdb.value;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.UniProtBlastResult;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier;

public class TestUniProtBlastResult {

  static final String default_test_file = FrameworkPropertyService.INSTANCE
      .getStringProperty("TEST_SEQ_SIM_FILE");

  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(default_test_file);
    System.out.println("Processing BLAST results file: " + testPath.toString());
    new TsvRecordSplitIteratorSupplier(testPath, UniProtBlastResult.Companion.getColumnHeadings())
        .get()
        .limit(100)
        .map(UniProtBlastResult.Companion::parseCSVRecord)
        .forEach(System.out::println);
  }
}
