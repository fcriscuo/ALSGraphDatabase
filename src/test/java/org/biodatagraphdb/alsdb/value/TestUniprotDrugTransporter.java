package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import org.biodatagraphdb.alsdb.model.UniProtDrugTransporter;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprotDrugTransporter {
  private static Logger log = Logger.get(TestUniprotDrugTransporter.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_transporter_uniprot_links.csv")).get()
        .limit(50)
        .map(UniProtDrugTransporter.Companion::parseCSVRecord)
        .forEach(transporter -> {
          log.info(transporter.getDrugModelType() + " id " +transporter.getId());
        });
  }
}
