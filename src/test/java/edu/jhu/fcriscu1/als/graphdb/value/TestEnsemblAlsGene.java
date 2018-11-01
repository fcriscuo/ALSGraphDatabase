package org.nygenome.als.graphdb.value;

import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsGene {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_genes.tsv"))
        .get()
        .map(EnsemblAlsGene::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
