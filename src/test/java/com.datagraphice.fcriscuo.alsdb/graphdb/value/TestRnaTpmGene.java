package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordSplitIteratorSupplier;

public class TestRnaTpmGene {
  static Logger log = Logger.get(TestRnaTpmGene.class);
  static final String DEFAULT_TEST_FILE = "/tmp/als_tpm.tsv";

  public static void main(String[] args) {
    Path testPath = (args.length>0) ? Paths.get(args[0])
        :Paths.get(DEFAULT_TEST_FILE);
    new TsvRecordSplitIteratorSupplier(testPath, edu.jhu.fcriscu1.als.graphdb.value.RnaTpmGene.columnHeadings())
        .get()
        .map(edu.jhu.fcriscu1.als.graphdb.value.RnaTpmGene::parseCsvRecordFunction)
        .limit(1000)
        .forEach(System.out::println);
  }


}
