package org.nygenome.als.graphdb.service;

/*
A singleton service to support
 */

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import javax.annotation.Nonnull;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.UniProtMapping;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum UniProtMappingService {
  INSTANCE;

  ImmutableMap<String, UniProtMapping> uniprotMap = Suppliers.memoize(new UniProtMapSupplier())
      .get();

  public Optional<UniProtMapping> getUniProtMappingByUniprotId(@Nonnull String id) {
    return (uniprotMap.containsKey(id)) ? Optional.of(uniprotMap.get(id))
        : Optional.empty();
  }

  public Optional<String> resolveGeneNameFromUniprotId(@Nonnull String id) {
    return (uniprotMap.containsKey(id)) ? Optional.of(uniprotMap.get(id).geneSymbol())
        : Optional.empty();
  }

  public Optional<UniProtMapping> resolveUniProtMappingFromGeneSymbol(@Nonnull String geneSymbol) {
    return uniprotMap
        .select(uniProtMapping -> uniProtMapping.geneSymbol().equalsIgnoreCase(geneSymbol))
        .stream()
        .findFirst();
  }

  public Optional<UniProtMapping> resolveUniProtMappingByEnsemblGeneId(
      @Nonnull String ensemblGeneId) {
    return uniprotMap
        .select(uniProtMapping -> uniProtMapping.ensemblGeneId().equalsIgnoreCase(ensemblGeneId))
        .stream()
        .findFirst();
  }

  public Optional<UniProtMapping> resolveUniProtMappingByEnsemblTranscriptId(
      @Nonnull String ensemblTranscriptId) {
    return uniprotMap
        .select(uniProtMapping -> uniProtMapping.ensemblTranscriptId()
            .equalsIgnoreCase(ensemblTranscriptId))
        .stream()
        .findFirst();
  }

  class UniProtMapSupplier implements Supplier<ImmutableMap<String, UniProtMapping>> {

    UniProtMapSupplier() {
    }

    private ImmutableMap<String, UniProtMapping> resolveMapFromFile() {
      Map<String, UniProtMapping> tmpMap = new HashMap<>();
      // TODO: make file name a property
      new TsvRecordStreamSupplier(Paths.get("/data/als/ensembl_uniprot_hgnc_map.tsv"))
          .get()
          .filter(record -> !Strings.isNullOrEmpty(record.get("UniProtKB/Swiss-Prot ID")))
          .forEach((record) ->
              tmpMap.put(record.get("UniProtKB/Swiss-Prot ID"),
                  UniProtMapping.parseCsvRecordFunction(record))
          );

      return Maps.immutable.ofMap(tmpMap);
    }

    @Override
    public ImmutableMap<String, UniProtMapping> get() {
      return resolveMapFromFile();
    }
  }

  // main method for stand alone testing
  public static void main(String[] args) {
    Lists.immutable.of("Q96P68", "P07203", "Q5JUX0")
        .stream()
        .forEach((id) -> UniProtMappingService.INSTANCE.getUniProtMappingByUniprotId(id)
            .ifPresent(System.out::println));
    // look for ENSG00000001461 should be gene NIPAL3
    System.out.println("Looking for gene id ENSG00000001461");
    UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId("ENSG00000001461")
        .ifPresent(System.out::println);
    System.out.println("Looking for transcript id: ENST00000419349");
    // should be gene GPX1
    UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblTranscriptId("ENST00000419349")
        .ifPresent(System.out::println);

    // look for uniprot id based on gene symbol
    Lists.immutable.of("NR2E3", "MIR875", "GNE", "FECD3")
        .stream()
        .peek((gs) -> System.out.println("Resolving gene symbol " + gs))
        .forEach(geneSymbol -> UniProtMappingService.INSTANCE
            .resolveUniProtMappingFromGeneSymbol(geneSymbol)
            .ifPresent(System.out::println));

  }
}
