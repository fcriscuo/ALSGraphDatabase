package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.lib.FunctionLib;
import java.nio.file.Path;

public class GeneUniprotIdAssociationDataConsumer extends GraphDataConsumer  {

    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map((line -> line.split(TAB_DELIM)))
                .forEach(this::processGeneUniprotData);
    }



    private void processGeneUniprotData(String[] tokens) {
        if (!proteintMap.containsKey(tokens[3])) {
            createProteinNode(tokens[0], tokens[3], strNoInfo,
                    tokens[4], strNoInfo, strNoInfo);
        } else {
            proteintMap.get(tokens[3]).setProperty("ProteinId",
                    tokens[0]);
        }
    }

}

