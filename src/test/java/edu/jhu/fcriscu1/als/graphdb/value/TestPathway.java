package edu.jhu.fcriscu1.als.graphdb.value;

import java.nio.file.Paths;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

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
