package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import org.biodatagraphdb.alsdb.model.Uniprot2Reactome;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;

public class TestUniprot2Reactome {
  private static Logger logger = Logger.get(TestUniprot2Reactome.class);
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/UniProt2Reactome.tsv"))
        .get()
        .map(Uniprot2Reactome.Companion::parseCSVRecord)
        .filter(u2r -> Uniprot2Reactome.Companion.isHumanSpecies(u2r.getSpecies()))
            //.filter(u2r ->u2r.species().equalsIgnoreCase("Homo sapiens"))
        .limit(100)
        .forEach(System.out::println);
  }
}
