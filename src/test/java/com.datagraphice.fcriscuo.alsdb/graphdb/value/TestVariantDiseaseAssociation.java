package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.twitter.logging.Logger;
import java.nio.file.Paths;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestVariantDiseaseAssociation {
 private static Logger logger = Logger.get(TestVariantDiseaseAssociation.class);
  public static void main(String[] args) {
    new TsvRecordStreamSupplier(Paths.get("/data/als/curated_variant_disease_associations.tsv")).get()
        .limit(50)
        .map(edu.jhu.fcriscu1.als.graphdb.value.VariantDiseaseAssociation::parseCSVRecord)
        .forEach(System.out::println);
  }

}
