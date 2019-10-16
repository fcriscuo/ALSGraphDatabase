package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier;

public class TestDrugBankValue {
  private final Logger log = Logger.get(TestDrugBankValue.class);

  public static void main(String[] args) {
    new CsvRecordStreamSupplier(Paths.get("/data/als/DrugBank/druglinks.csv"))
        .get()
        .limit(50)
        .map(edu.jhu.fcriscu1.als.graphdb.value.DrugBankValue::parseCSVRecord)
        .forEach(System.out::println);
  }

}
