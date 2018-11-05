package edu.jhu.fcriscu1.als.graphdb.value;

import java.nio.file.Paths;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;

public class TestEnsemblAlsSnp {

  public static void main(String[] args) {
    //TODO: make this a property
    new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_als_snp.tsv"))
        .get()
        .map(EnsemblAlsSnp::parseCSVRecord)
        .limit(100)
        .forEach(System.out::println);
  }

}
