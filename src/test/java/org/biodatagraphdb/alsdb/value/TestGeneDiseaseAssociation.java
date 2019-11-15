package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import org.biodatagraphdb.alsdb.value.GeneDiseaseAssociation;
import java.nio.file.Paths;

public class TestGeneDiseaseAssociation {
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_gene_disease_associations.tsv")).get()
        .limit(50)
        .map(GeneDiseaseAssociation::parseCSVRecord)
        .forEach(System.out::println);
  }
}
