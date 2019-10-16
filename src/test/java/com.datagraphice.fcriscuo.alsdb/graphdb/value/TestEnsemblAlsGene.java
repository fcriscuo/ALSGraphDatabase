package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsGene {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_genes.tsv"))
        .get()
        .map(edu.jhu.fcriscu1.als.graphdb.value.EnsemblAlsGene::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
