package org.nygenome.als.graphdb.value;

import com.twitter.logging.Logger;

import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;

import java.nio.file.Paths;

public class TestUniProtDrugEnzyme {
  static private Logger log = Logger.get(TestUniProtDrugEnzyme.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_enzyme_uniprot_links.csv")).get()
        .limit(50)
        .map(UniProtDrugEnzyme::parseCSVRecord)
        .forEach(target -> {
          log.info(target.drugModelType() + " id " +target.id());
        });
  }
}
