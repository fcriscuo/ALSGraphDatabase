package org.biodatagraphdb.alsdb.service;

/*
A singleton service to support
 */

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.biodatagraphdb.alsdb.model.UniProtMapping;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;

import javax.annotation.Nonnull;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum UniProtMappingService {
    INSTANCE;

    ImmutableMap<String, org.biodatagraphdb.alsdb.model.UniProtMapping> uniprotMap = Suppliers.memoize(new UniProtMapSupplier())
            .get();

    public Optional<org.biodatagraphdb.alsdb.model.UniProtMapping> getUniProtMappingByUniprotId(@Nonnull String id) {
        return (uniprotMap.containsKey(id)) ? Optional.of(uniprotMap.get(id))
                : Optional.empty();
    }

    public Optional<String> resolveGeneNameFromUniprotId(@Nonnull String id) {
        return (uniprotMap.containsKey(id)) ? Optional.of(uniprotMap.get(id).getGeneSymbol())
                : Optional.empty();
    }

    public Optional<org.biodatagraphdb.alsdb.model.UniProtMapping> resolveUniProtMappingFromGeneSymbol(@Nonnull String geneSymbol) {
        return uniprotMap
                .select(uniProtMapping -> uniProtMapping.getGeneSymbol().equalsIgnoreCase(geneSymbol))
                .stream()
                .findFirst();
    }

    public Optional<org.biodatagraphdb.alsdb.model.UniProtMapping> resolveUniProtMappingByEnsemblGeneId(
            @Nonnull String ensemblGeneId) {
        return uniprotMap
                .select(uniProtMapping -> uniProtMapping.getEnsemblGeneId().equalsIgnoreCase(ensemblGeneId))
                .stream()
                .findFirst();
    }

    public Optional<org.biodatagraphdb.alsdb.model.UniProtMapping> resolveUniProtMappingByEnsemblTranscriptId(
            @Nonnull String ensemblTranscriptId) {
        return uniprotMap
                .select(uniProtMapping -> uniProtMapping.getEnsemblTranscriptId()
                        .equalsIgnoreCase(ensemblTranscriptId))
                .stream()
                .findFirst();
    }

    class UniProtMapSupplier implements Supplier<ImmutableMap<String, org.biodatagraphdb.alsdb.model.UniProtMapping>> {

        UniProtMapSupplier() {
        }

        private ImmutableMap<String, org.biodatagraphdb.alsdb.model.UniProtMapping> resolveMapFromFile() {
            Map<String, org.biodatagraphdb.alsdb.model.UniProtMapping> tmpMap = new HashMap<>();
            // TODO: make file name a property
            new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier
                    (Paths.get(org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getStringProperty("ENSEMBL_UNIPROT_HGNC_FILE")))
                    .get()
                    .filter(record -> !Strings.isNullOrEmpty(record.get("UniProtKB/Swiss-Prot ID")))
                    .map(UniProtMapping.Companion::parseCsvRecordFunction)
                    .forEach((uniProtMapping) ->
                            tmpMap.put(uniProtMapping.getUniProtId(),
                                    uniProtMapping)
                    );

            return Maps.immutable.ofMap(tmpMap);
        }

        @Override
        public ImmutableMap<String, org.biodatagraphdb.alsdb.model.UniProtMapping> get() {
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
        System.out.println("Looking for gene id ENSG00000261457");
        UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblGeneId("ENSG00000261457")
                .ifPresent(System.out::println);
        System.out.println("Looking for transcript id: ENST00000419349");
        // should be gene GPX1
        UniProtMappingService.INSTANCE.resolveUniProtMappingByEnsemblTranscriptId("ENST00000419349")
                .ifPresent(System.out::println);

        // look for uniprot id based on gene symbol
        Lists.immutable.of("AC008417.1", "NR2E3", "MIR875", "GNE", "FECD3")
                .stream()
                .peek((gs) -> System.out.println("Resolving gene symbol " + gs))
                .forEach(geneSymbol -> UniProtMappingService.INSTANCE
                        .resolveUniProtMappingFromGeneSymbol(geneSymbol)
                        .ifPresent(System.out::println));

    }
}
