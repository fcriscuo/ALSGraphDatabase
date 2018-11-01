package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.GeneDiseaseAssociation;
import java.nio.file.Paths;

public class TestGeneDiseaseAssociation {
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_gene_disease_associations.tsv")).get()
        .limit(50)
        .map(GeneDiseaseAssociation::parseCSVRecord)
        .forEach(System.out::println);
  }
}
