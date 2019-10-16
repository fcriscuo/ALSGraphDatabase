package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;

public class TestUniProtDrug {
 static private Logger log = Logger.get(TestUniProtDrug.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_carrier_uniprot_links.csv")).get()
        .limit(200)
        .map(edu.jhu.fcriscu1.als.graphdb.value.UniProtDrug::parseCSVRecord)
        .forEach(drug -> {
          log.info("uniprot " +drug.uniprotId() + " id " +drug.id());
          drug.drugIdList().forEach(id -> log.info(" drug " +id));
        });
  }

}
