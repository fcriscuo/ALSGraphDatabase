package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestHgncLocus {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/HGNC/test_hgnc_complete_set.txt"))
        .get()
        .map(org.biodatagraphdb.alsdb.value.HgncLocus::parseCSVRecord)
        .filter(org.biodatagraphdb.alsdb.value.HgncLocus::isApprovedLocus)
        .filter(org.biodatagraphdb.alsdb.value.HgncLocus::isApprovedLocusTypeGroup)
        .limit(500)
        .forEach(System.out::println);
  }

}
