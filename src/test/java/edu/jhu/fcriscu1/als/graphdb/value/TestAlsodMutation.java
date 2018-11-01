package org.nygenome.als.graphdb.value;

import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestAlsodMutation {
 public static void main(String[] args) {
   FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
       .ifPresent( path ->
           new TsvRecordStreamSupplier(path)
       .get()
       .map(AlsodMutation::parseCSVRecord)
       .limit(500)
       .forEach(System.out::println));
 }
}
