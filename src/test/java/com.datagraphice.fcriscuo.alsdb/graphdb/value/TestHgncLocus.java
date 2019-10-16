package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestHgncLocus {
  public static void main(String[] args) {
    //TODO: make filename a property
    new TsvRecordStreamSupplier(Paths.get("/data/HGNC/test_hgnc_complete_set.txt"))
        .get()
        .map(edu.jhu.fcriscu1.als.graphdb.value.HgncLocus::parseCSVRecord)
        .filter(edu.jhu.fcriscu1.als.graphdb.value.HgncLocus::isApprovedLocus)
        .filter(edu.jhu.fcriscu1.als.graphdb.value.HgncLocus::isApprovedLocusTypeGroup)
        .limit(500)
        .forEach(System.out::println);
  }

}
