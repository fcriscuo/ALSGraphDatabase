package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import org.biodatagraphdb.alsdb.model.UniProtDrugCarrier;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniProtDrugCarrier {
 static private Logger log = Logger.get(TestUniProtDrugCarrier.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_carrier_uniprot_links.csv")).get()
        .limit(200)
        .map(UniProtDrugCarrier.Companion::parseCSVRecord)
        .forEach(target -> {
          log.info(target.getDrugModelType() + " id " +target.getId());
          target.getDrugIdList().forEach(id -> log.info(" drug " +id));
        });
  }

}