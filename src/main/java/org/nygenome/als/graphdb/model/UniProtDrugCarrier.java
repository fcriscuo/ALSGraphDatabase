package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;

import java.nio.file.Paths;
import java.util.function.Function;

@Log4j

public class UniProtDrugCarrier extends AbstractUniProtDrugModel{

  public UniProtDrugCarrier() {
    this.drugModelType = "DRUG_CARRIER";
  }


  public static Function<CSVRecord, UniProtDrugCarrier> parseCsvRecordFunction = (record) -> {
    UniProtDrugCarrier target = new UniProtDrugCarrier();
    target.parseCSVRecord(record);
    return target;
  };


  // main method for stand alone testing
  public static void main(String[] args) {
      new CsvRecordStreamSupplier(Paths.get("/data/als/drug_carrier_uniprot_links.csv")).get()
          .limit(50)
          .map(parseCsvRecordFunction)
          .forEach(target -> {
            log.info(target.drugModelType + " id " +target.getId());
          });
  }

}
