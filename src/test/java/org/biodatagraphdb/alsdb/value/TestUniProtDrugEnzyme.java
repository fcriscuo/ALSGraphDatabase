package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;

import org.biodatagraphdb.alsdb.model.UniProtDrugEnzyme;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;

import java.nio.file.Paths;

public class TestUniProtDrugEnzyme {
  static private Logger log = Logger.get(TestUniProtDrugEnzyme.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_enzyme_uniprot_links.csv")).get()
        .limit(50)
        .map(UniProtDrugEnzyme.Companion::parseCSVRecord)
        .forEach(target -> {
          log.info(target.getDrugModelType() + " id " +target.getId());
        });
  }
}
