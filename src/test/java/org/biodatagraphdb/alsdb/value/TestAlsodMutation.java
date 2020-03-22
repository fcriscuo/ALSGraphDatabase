package org.biodatagraphdb.alsdb.value;

import org.biodatagraphdb.alsdb.model.AlsodMutation;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;

public class TestAlsodMutation {
 public static void main(String[] args) {
   FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
       .ifPresent( path ->
           new TsvRecordStreamSupplier(path)
       .get()
       .map(AlsodMutation.Companion::parseCSVRecord)
       .limit(500)
       .forEach(System.out::println));
 }
}
