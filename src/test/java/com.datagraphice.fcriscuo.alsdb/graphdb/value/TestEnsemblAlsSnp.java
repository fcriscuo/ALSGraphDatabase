package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsSnp {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_snp.tsv"))
        .get()
        .map(edu.jhu.fcriscu1.als.graphdb.value.EnsemblAlsSnp::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
