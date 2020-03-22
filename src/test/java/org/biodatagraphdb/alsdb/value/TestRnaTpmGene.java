package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier;
import org.biodatagraphdb.alsdb.model.RnaTpmGene;

public class TestRnaTpmGene {
  static Logger log = Logger.get(TestRnaTpmGene.class);
  static final String DEFAULT_TEST_FILE = "/tmp/als_tpm.tsv";

  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(DEFAULT_TEST_FILE);
    new TsvRecordSplitIteratorSupplier(testPath, RnaTpmGene.Companion.getColumnHeadings())
        .get()
        .map(RnaTpmGene.Companion::parseCsvRecordFunction)
        .limit(100)
        .forEach(System.out::println);
  }


}
