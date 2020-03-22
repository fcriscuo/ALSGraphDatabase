package org.biodatagraphdb.alsdb.value;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.SampleVariantSummary;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier;

public class TestSampleVariantSummary {
  static final String default_test_file = FrameworkPropertyService.INSTANCE
      .getStringProperty("TEST_SAMPLE_VARIANT_SUMUMMARY_FILE");
  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(default_test_file);
    System.out.println("Processing sample variants summary  file: " + testPath.toString());
    new TsvRecordSplitIteratorSupplier(testPath, SampleVariantSummary.Companion.getColumnHeadings())
        .get()
        .limit(100)
        .map(SampleVariantSummary.Companion::parseCSVRecord)
        .forEach(var -> {
          System.out.println("id: "+ var.getId()  + "   " +
              var.getHugoGeneName() +" number of variants= " +
              var.getNumVariants());

        });

  }

}
