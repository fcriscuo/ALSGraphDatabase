package org.nygenome.als.graphdb.value;

import com.twitter.logging.Logger;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprot2Reactome {
  private static Logger logger = Logger.get(TestUniprot2Reactome.class);
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(Uniprot2Reactome::parseCSVRecord)
        .filter(u2r ->Uniprot2Reactome.isHuman(u2r.species()))
        .limit(100)
        .forEach(System.out::println);
  }
}
