package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.EnsemblAlsSnp;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsSnp {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_snp.tsv"))
        .get()
        .map(EnsemblAlsSnp.Companion::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
