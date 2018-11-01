package org.nygenome.als.graphdb.value;

import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestHgncLocus {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/HGNC/test_hgnc_complete_set.txt"))
        .get()
        .map(HgncLocus::parseCSVRecord)
        .filter(HgncLocus::isApprovedLocus)
        .filter(HgncLocus::isApprovedLocusTypeGroup)
        .limit(500)
        .forEach(System.out::println);
  }

}
