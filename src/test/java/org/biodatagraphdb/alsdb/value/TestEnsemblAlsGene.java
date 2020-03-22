package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.EnsemblAlsGene;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsGene {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_genes.tsv"))
        .get()
        .map(EnsemblAlsGene.Companion::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}