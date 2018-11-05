package edu.jhu.fcriscu1.als.graphdb.value;

import com.twitter.logging.Logger;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprot2Reactome {
  private static Logger logger = Logger.get(TestUniprot2Reactome.class);
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(Uniprot2Reactome::parseCSVRecord)
        .filter(u2r ->Uniprot2Reactome.isHuman(u2r.species()))
            //.filter(u2r ->u2r.species().equalsIgnoreCase("Homo sapiens"))
        .limit(100)
        .forEach(System.out::println);
  }
}
