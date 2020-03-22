package org.biodatagraphdb.alsdb.value;

import java.nio.file.Paths;

import org.biodatagraphdb.alsdb.model.HgncLocus;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestHgncLocus {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/HGNC/test_hgnc_complete_set.txt"))
        .get()
        .map(HgncLocus.Companion::parseCSVRecord)
        .filter(HgncLocus::isApprovedLocus)
        .filter(HgncLocus::isApprovedLocusTypeGroup)
        .limit(500)
        .forEach(System.out::println);
  }

}
