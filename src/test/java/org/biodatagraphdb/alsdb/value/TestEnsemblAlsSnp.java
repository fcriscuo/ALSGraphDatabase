package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsSnp {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_snp.tsv"))
        .get()
        .map(org.biodatagraphdb.alsdb.value.EnsemblAlsSnp::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
