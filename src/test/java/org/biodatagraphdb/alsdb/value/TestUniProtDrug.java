package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.UniProtDrug;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;

public class TestUniProtDrug {
 static private Logger log = Logger.get(TestUniProtDrug.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_carrier_uniprot_links.csv")).get()
        .limit(200)
        .map(UniProtDrug.Companion::parseCSVRecord)
        .forEach(drug -> {
          log.info("uniprot " +drug.getUniprotId() + " id " +drug.getId());
          drug.getDrugIdList().forEach(id -> log.info(" drug " +id));
        });
  }

}
