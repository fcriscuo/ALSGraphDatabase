package org.nygenome.als.graphdb.consumer;


import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.model.HumanTissueAtlas;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class HumanTissueAtlasDataConsumer extends GraphDataConsumer {
    //readTissueInfo(HUMAN_TISSUE_ATLAS);

    private Predicate<HumanTissueAtlas> reliabilityPredicate = (ht) ->
        ht.getReliability().equalsIgnoreCase("Approved")
        || ht.getReliability().equalsIgnoreCase("Supported");

    @Override
    public void accept(Path path) {
        new TsvRecordStreamSupplier(path)
            .get()
            .map(HumanTissueAtlas.parseCsvRecordFunction)
            // filter out records based on reliability value
            .filter(reliabilityPredicate)
            .forEach(consumeHumanTissueAtlasObject);
    }

    private Predicate<String> novelTissueFilterPredicate = (tissue) ->
        !tissueMap.containsKey(tissue);

    private Consumer<HumanTissueAtlas> consumeHumanTissueAtlasObject = (ht) -> {
        // if this is a new tissue type - create a Map entry
        Arrays.asList( ht.getTissue().split("[:;]"))
            .stream()
            .filter(novelTissueFilterPredicate)
            .forEach(this::createTissueNode);
        // create an association between this tissue type and the ensembl id
        createEnsemblTissueAssociation(ht);

    };
    private void processHumanTissueAtlasData(String[] tokens) {
        String[] szTissueTokens = tokens[2].split("[:;]");
        for (int i = 0; i < szTissueTokens.length; i = i + 2) {
            if (!tissueMap.containsKey(szTissueTokens[i])) {
                createTissueNode(szTissueTokens[i]);
            }
        }
        createEnsemblTissueAssociation(tokens);
    }
}
