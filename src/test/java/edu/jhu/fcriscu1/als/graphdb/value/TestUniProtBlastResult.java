package edu.jhu.fcriscu1.als.graphdb.value;

import java.nio.file.Path;
import java.nio.file.Paths;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordSplitIteratorSupplier;

public class TestUniProtBlastResult {

  static final String default_test_file = FrameworkPropertyService.INSTANCE
      .getStringProperty("TEST_SEQ_SIM_FILE");

  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(default_test_file);
    System.out.println("Processing BLAST results file: " + testPath.toString());
    new TsvRecordSplitIteratorSupplier(testPath,UniProtBlastResult.columnHeadings())
        .get()
        .limit(1000)
        .map(UniProtBlastResult::parseCSVRecord)
        .forEach(System.out::println);
  }
}
