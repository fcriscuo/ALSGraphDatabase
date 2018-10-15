package org.nygenome.als.graphdb.value;

import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestEnsemblGene {

  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/test_ensembl_gene_info_GRCh38.tsv"))
        .get()
        .map(EnsemblGene::parseCSVRecord)
        .filter(gene -> !gene.chromosome().equalsIgnoreCase("MT"))
        .limit(100)
        .forEach(System.out::println);
  }

}
