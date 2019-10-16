package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprotDrugTarget {
  private static Logger log = Logger.get(TestUniprotDrugTarget.class);

  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_target_uniprot_links.csv")).get()
        .limit(50)
        .map(edu.jhu.fcriscu1.als.graphdb.value.UniProtDrugTarget::parseCSVRecord)
        .forEach(target -> {
          log.info(target.drugModelType() + " id " + target.id());
        });
  }
}
