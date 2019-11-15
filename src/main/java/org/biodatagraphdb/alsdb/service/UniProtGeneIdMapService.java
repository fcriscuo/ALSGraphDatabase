package org.biodatagraphdb.alsdb.service;

import javax.annotation.Nonnull;
import lombok.extern.log4j.Log4j;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/*
Singleton service class that supports bidirectional mapping between
UniProt protein id and gene symbol
 */
@Log4j
public enum UniProtGeneIdMapService {
  INSTANCE;

  private final ImmutableMap<String,Integer> uniProtIdToGenIdMap = new UniprotIdMapSupplier().get();

  public Optional<Integer>  resolveGeneIdFromUniProtId(@Nonnull String uniProtId){
    return (uniProtIdToGenIdMap.containsKey(uniProtId))
        ? Optional.of(uniProtIdToGenIdMap.get(uniProtId))
        : Optional.empty();
  }


  class UniprotIdMapSupplier implements Supplier<ImmutableMap<String,Integer>> {
     UniprotIdMapSupplier() { }

    @Override public ImmutableMap<String, Integer> get() {
      return resolveMapFromFile();
    }
    private ImmutableMap<String,Integer> resolveMapFromFile(){
      Map<String,Integer> tmpMap = new HashMap<>();
       new TsvRecordStreamSupplier(Paths.get("/data/als/mapa_geneid_4_uniprot_crossref.tsv"))
           .get()
           .forEach((record) ->
                 tmpMap.put(record.get("UniProtKB"), Integer.valueOf(record.get("GENEID")))
               );
       return Maps.immutable.ofMap(tmpMap);

    }
  }

  public static void main(String[] args) {
    UniProtGeneIdMapService.INSTANCE.resolveGeneIdFromUniProtId("P62158").ifPresent((id) ->
        log.info("P62158 should resolve to 801  result is: " +id));


  }

}
