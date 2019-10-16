package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestPathway {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(edu.jhu.fcriscu1.als.graphdb.value.Pathway::parseCSVRecord)
        .filter(pathway-> edu.jhu.fcriscu1.als.graphdb.value.Pathway.isHuman(pathway.species()))
        .limit(50)
        .forEach(System.out::println);
  }

}
