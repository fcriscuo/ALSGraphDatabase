package org.nygenome.als.graphdb.value;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;

public class TestSampleVariantSummary {
  static final String default_test_file = FrameworkPropertyService.INSTANCE
      .getStringProperty("TEST_SAMPLE_VAR_SUMM_FILE");
  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(default_test_file);
    System.out.println("Processing sample variants summary  file: " + testPath.toString());
    new TsvRecordSplitIteratorSupplier(testPath,SampleVariantSummary.columnHeadings())
        .get()
        .limit(1000)
        .map(SampleVariantSummary::parseCSVRecord)
        .forEach(System.out::println);

  }

}
