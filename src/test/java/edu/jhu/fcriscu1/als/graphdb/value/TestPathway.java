package org.nygenome.als.graphdb.value;

import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestPathway {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(Pathway::parseCSVRecord)
        .filter(pathway-> Pathway.isHuman(pathway.species()))
        .limit(50)
        .forEach(System.out::println);
  }

}
