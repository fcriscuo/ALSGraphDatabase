package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier;

public class TestRnaTpmGene {
  static Logger log = Logger.get(TestRnaTpmGene.class);
  static final String DEFAULT_TEST_FILE = "/tmp/als_tpm.tsv";

  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(DEFAULT_TEST_FILE);
    new TsvRecordSplitIteratorSupplier(testPath, org.biodatagraphdb.alsdb.value.RnaTpmGene.columnHeadings())
        .get()
        .map(org.biodatagraphdb.alsdb.value.RnaTpmGene::parseCsvRecordFunction)
        .limit(1000)
        .forEach(System.out::println);
  }


}
