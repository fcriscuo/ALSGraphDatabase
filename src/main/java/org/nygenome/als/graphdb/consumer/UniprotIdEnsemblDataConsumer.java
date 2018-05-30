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

        if (!proteintMap.containsKey(tokens[0])) {
            createProteinNode(strNoInfo, tokens[0], strNoInfo,
                    strNoInfo, strNoInfo, strNoInfo);
        }
        proteintMap.get(tokens[0]).setProperty("EnsemblTranscript",
                tokens[1]);
        proteintMap.get(tokens[0])
                .setProperty("ProteinName", tokens[2]);
        proteintMap.get(tokens[0]).setProperty("GeneSymbol", tokens[3]);
    }

}

