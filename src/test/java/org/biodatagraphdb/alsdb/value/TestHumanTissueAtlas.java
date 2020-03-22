package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.HumanTissueAtlas;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestHumanTissueAtlas {
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/HumanTissueAtlas.tsv"))
        .get()
        .limit(50)
        .map(HumanTissueAtlas.Companion::parseCSVRecord)
        .forEach(System.out::println);
  }
}
