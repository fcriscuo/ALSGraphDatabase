package org.nygenome.als.graphdb.consumer;


import org.nygenome.als.graphdb.lib.FunctionLib;

import java.nio.file.Path;

public class HumanTissueAtlasDataConsumer extends GraphDataConsumer {
    //readTissueInfo(HUMAN_TISSUE_ATLAS);
    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map(line -> line.replace(strApostrophe, ""))
                .map((line -> line.split(TAB_DELIM)))
                .map(FunctionLib.processTokensFunction::apply)
                .forEach(this::processHumanTissueAtlasData);
    }
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
