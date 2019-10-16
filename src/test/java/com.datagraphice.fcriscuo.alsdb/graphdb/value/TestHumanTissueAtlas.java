package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestHumanTissueAtlas {
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/HumanTissueAtlas.tsv"))
        .get()
        .limit(50)
        .map(record -> edu.jhu.fcriscu1.als.graphdb.value.HumanTissueAtlas.parseCSVRecord(record))
        .forEach(System.out::println);
  }
}
