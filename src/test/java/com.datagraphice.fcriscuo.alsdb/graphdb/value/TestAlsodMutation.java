package com.datagraphice.fcriscuo.alsdb.graphdb.value;

import com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService;
import com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier;

public class TestAlsodMutation {
 public static void main(String[] args) {
   FrameworkPropertyService.INSTANCE.getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
       .ifPresent( path ->
           new TsvRecordStreamSupplier(path)
       .get()
       .map(edu.jhu.fcriscu1.als.graphdb.value.AlsodMutation::parseCSVRecord)
       .limit(500)
       .forEach(System.out::println));
 }
}
