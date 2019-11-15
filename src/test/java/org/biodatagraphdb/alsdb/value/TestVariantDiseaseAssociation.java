package org.biodatagraphdb.alsdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestVariantDiseaseAssociation {
 private static Logger logger = Logger.get(TestVariantDiseaseAssociation.class);
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_variant_disease_associations.tsv")).get()
        .limit(50)
        .map(org.biodatagraphdb.alsdb.value.VariantDiseaseAssociation::parseCSVRecord)
        .forEach(System.out::println);
  }

}
