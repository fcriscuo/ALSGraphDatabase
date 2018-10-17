package org.nygenome.als.graphdb.value;

import java.nio.file.Paths;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

public class TestAlsodMutation {
 public static void main(String[] args) {
   //TODO: make filename a property
   new TsvRecordStreamSupplier(Paths.get("/data/als/alsod/Alsod_Mutation_Data.txt"))
       .get()
       .map(AlsodMutation::parseCSVRecord)
       .limit(500)
       .forEach(System.out::println);
 }
}
