package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestPathway {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(org.biodatagraphdb.alsdb.value.Pathway::parseCSVRecord)
        .filter(pathway-> org.biodatagraphdb.alsdb.value.Pathway.isHuman(pathway.species()))
        .limit(50)
        .forEach(System.out::println);
  }

}
