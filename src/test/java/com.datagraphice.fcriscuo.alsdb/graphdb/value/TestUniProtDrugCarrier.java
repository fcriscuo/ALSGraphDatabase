package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniProtDrugCarrier {
 static private Logger log = Logger.get(TestUniProtDrugCarrier.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_carrier_uniprot_links.csv")).get()
        .limit(200)
        .map(edu.jhu.fcriscu1.als.graphdb.value.UniProtDrugCarrier::parseCSVRecord)
        .forEach(target -> {
          log.info(target.drugModelType() + " id " +target.id());
          target.drugIdList().forEach(id -> log.info(" drug " +id));
        });
  }

}
