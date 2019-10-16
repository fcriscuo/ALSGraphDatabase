package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprotDrugTransporter {
  private static Logger log = Logger.get(TestUniprotDrugTransporter.class);
  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/drug_transporter_uniprot_links.csv")).get()
        .limit(50)
        .map(edu.jhu.fcriscu1.als.graphdb.value.UniProtDrugTransporter::parseCSVRecord)
        .forEach(transporter -> {
          log.info(transporter.drugModelType() + " id " +transporter.id());
        });
  }
}
