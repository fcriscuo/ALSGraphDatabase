package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.lib.FunctionLib;

import java.nio.file.Path;

public class UniprotIdEnsemblDataConsumer extends GraphDataConsumer{

    @Override
    public void accept(Path path) {

        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map((line -> line.split(TAB_DELIM)))
                .map(FunctionLib.processTokensFunction::apply)
                .forEach(this::processUniprotIdEnsemblData);
    }

    private void processUniprotIdEnsemblData(String[] tokens) {

        if (!proteinMap.containsKey(tokens[0])) {
            createProteinNode(strNoInfo, tokens[0], strNoInfo,
                    strNoInfo, strNoInfo, strNoInfo);
        }
        proteinMap.get(tokens[0]).setProperty("EnsemblTranscript",
                tokens[1]);
        proteinMap.get(tokens[0])
                .setProperty("ProteinName", tokens[2]);
        proteinMap.get(tokens[0]).setProperty("GeneSymbol", tokens[3]);
    }

}

