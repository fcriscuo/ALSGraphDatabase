package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import java.nio.file.Path;
import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordSplitIteratorSupplier;

public class TestSampleVariantSummary {
  static final String default_test_file = FrameworkPropertyService.INSTANCE
      .getStringProperty("TEST_SAMPLE_VARIANT_SUMUMMARY_FILE");
  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(default_test_file);
    System.out.println("Processing sample variants summary  file: " + testPath.toString());
    new TsvRecordSplitIteratorSupplier(testPath, edu.jhu.fcriscu1.als.graphdb.value.SampleVariantSummary.columnHeadings())
        .get()
        .limit(100)
        .map(edu.jhu.fcriscu1.als.graphdb.value.SampleVariantSummary::parseCSVRecord)
        .forEach(var -> {
          System.out.println("id: "+ var.id()  + "   " +
              var.hugoGeneName() +" number of variants= " +
              var.numVariants());

        });

  }

}
