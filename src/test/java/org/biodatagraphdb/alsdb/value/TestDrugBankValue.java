package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.DrugBankValue;
import org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier;

public class TestDrugBankValue {
  private final Logger log = Logger.get(TestDrugBankValue.class);

  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/DrugBank/druglinks.csv"))
        .get()
        .limit(50)
        .map(DrugBankValue.Companion::parseCSVRecord)
        .forEach(System.out::println);
  }

}
