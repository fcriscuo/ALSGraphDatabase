package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import org.biodatagraphdb.alsdb.model.UniProtDrugTarget;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprotDrugTarget {
  private static Logger log = Logger.get(TestUniprotDrugTarget.class);

  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_target_uniprot_links.csv")).get()
        .limit(50)
        .map(UniProtDrugTarget.Companion::parseCSVRecord)
        .forEach(target -> {
          log.info(target.getDrugModelType() + " id " + target.getId());
        });
  }
}
