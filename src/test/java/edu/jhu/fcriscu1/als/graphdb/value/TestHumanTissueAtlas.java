package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestHumanTissueAtlas {
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/HumanTissueAtlas.tsv"))
        .get()
        .limit(50)
        .map(record ->HumanTissueAtlas.parseCSVRecord(record))
        .forEach(System.out::println);
  }
}
