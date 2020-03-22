package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.Pathway;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestPathway {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(org.biodatagraphdb.alsdb.model.Pathway.Companion::parseCSVRecord)
        .filter(Pathway::isHuman)
        .limit(50)
        .forEach(System.out::println);
  }

}
