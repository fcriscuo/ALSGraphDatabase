package org.nygenome.als.graphdb.model;

import org.apache.commons.csv.CSVRecord;

import lombok.extern.log4j.Log4j;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;

import java.nio.file.Paths;
import java.util.function.Function;

@Log4j

public class UniProtDrugEnzyme extends AbstractUniProtDrugModel{

  public UniProtDrugEnzyme() {
    this.drugModelType = "DRUG_ENZYME";
  }


  public static Function<CSVRecord, UniProtDrugEnzyme> parseCsvRecordFunction = (record) -> {
    UniProtDrugEnzyme target = new UniProtDrugEnzyme();
    target.parseCSVRecord(record);
    return target;
  };


  // main method for stand alone testing
  public static void main(String[] args) {
      new CsvRecordStreamSupplier(Paths.get("/data/als/drug_enzyme_uniprot_links.csv")).get()
          .limit(50)
          .map(parseCsvRecordFunction)
          .forEach(target -> {
            log.info(target.drugModelType + " id " +target.getId());
          });
  }

}
