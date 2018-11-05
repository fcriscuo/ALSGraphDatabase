package edu.jhu.fcriscu1.als.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;
import edu.jhu.fcriscu1.als.graphdb.util.CsvRecordStreamSupplier;

public class TestDrugBankValue {
  private final Logger log = Logger.get(TestDrugBankValue.class);

  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/DrugBank/druglinks.csv"))
        .get()
        .limit(50)
        .map(DrugBankValue::parseCSVRecord)
        .forEach(System.out::println);
  }

}
