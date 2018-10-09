package org.nygenome.als.graphdb.service;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.AlsAssociatedGene;

/*
Java Singleton that supports a white list of genes that
have been associated with ALS
This service class supports annotation of genetic data
imported into the database
 */
public enum AlsAssociatedGeneService {
  INSTANCE;

private Path alsGeneFilePath = Paths.get(
    FrameworkPropertyService.INSTANCE.getStringProperty("ALS_GENE_WHITE_LIST_FILE"));

  private final ImmutableMap<String,AlsAssociatedGene> alsAssociatedGeneMap =
      Suppliers.memoize(new  AlsAssociateGeneMapSupplier(alsGeneFilePath)).get();

  public Predicate<String> alsAssociatedGenePedicate = (@Nonnull String name) ->
      alsAssociatedGeneMap.containsKey(name.toUpperCase());

  public Function<String, Optional<AlsAssociatedGene>> resolveAlsAssociatedGeneFunction =
      (@Nonnull String name) ->
          (alsAssociatedGenePedicate.test(name))
      ? Optional.of(alsAssociatedGeneMap.get(name.toUpperCase()))
              : Optional.empty();

  class AlsAssociateGeneMapSupplier implements Supplier<ImmutableMap<String, AlsAssociatedGene>> {
    private final Map<String,AlsAssociatedGene> alsGeneMap  = Maps.mutable.empty();

    AlsAssociateGeneMapSupplier(@Nonnull Path tsvFilePath) {
      generateMap(tsvFilePath);
    }

    private void generateMap(Path tsvFilePath) {
      new TsvRecordStreamSupplier(tsvFilePath)
          .get()
          .map(AlsAssociatedGene::parseCSVRecord)
          .forEach(gene -> alsGeneMap.put(gene.geneSymbol(),gene));
    }

    @Override
    public ImmutableMap<String, AlsAssociatedGene> get() {
     return Maps.immutable.ofMap(this.alsGeneMap);
    }
  }

  public static void main(String[] args) {
    // test some gene names
    // BRCA1 is not an ALS gene
    // XXXXXXX is not a valid gene name
    Stream.of("CST3", "PCP4", "BRCA1", "SOD1","XXXXXXX")
        .forEach(name -> {
          AlsAssociatedGeneService.INSTANCE.resolveAlsAssociatedGeneFunction.apply(name)
              .ifPresent(System.out::println);
        });

  }

}
