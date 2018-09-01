package org.nygenome.als.graphdb.consumer;



import org.nygenome.als.graphdb.value.HumanTissueAtlas;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class HumanTissueAtlasDataConsumer extends GraphDataConsumer {
    //readTissueInfo(HUMAN_TISSUE_ATLAS);

    private Predicate<HumanTissueAtlas> reliabilityPredicate = (ht) ->
        ht.reliability().equalsIgnoreCase("Approved")
        || ht.reliability().equalsIgnoreCase("Supported");

  private Predicate<HumanTissueAtlas> levelPredicate = (ht) ->
      !ht.level().equalsIgnoreCase("Not detected");

    @Override
    public void accept(Path path) {
        new TsvRecordStreamSupplier(path)
            .get()
            .map(record->HumanTissueAtlas.parseCSVRecord(record))
            // filter out records based on reliability value
            .filter(reliabilityPredicate)
            .filter(levelPredicate)
            .forEach(consumeHumanTissueAtlasObject);
    }





    private Consumer<HumanTissueAtlas> consumeHumanTissueAtlasObject = (ht) -> {

        createEnsemblTissueAssociation(ht);

    };
    private void processHumanTissueAtlasData(String[] tokens) {
//        String[] szTissueTokens = tokens[2].split("[:;]");
//        for (int i = 0; i < szTissueTokens.length; i = i + 2) {
//            if (!tissueMap.containsKey(szTissueTokens[i])) {
//                createTissueNode(szTissueTokens[i]);
//            }
//        }
        createEnsemblTissueAssociation(tokens);
    }
}
