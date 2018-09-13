package org.nygenome.als.graphdb.consumer;


import org.apache.log4j.Logger;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.EmbeddedGraph;
import scala.Tuple2;
import java.nio.file.Path;


public class SeqSimilarityDataConsumer extends GraphDataConsumer {
    private static final Logger log = Logger.getLogger(SeqSimilarityDataConsumer.class);

    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map((line -> line.split(TAB_DELIM)))
                .map(FunctionLib.processTokensFunction::apply)
                .forEach(this::processSeqSimilarityData);
    }
    
    private void processSeqSimilarityData(String[] tokens){
        if (!tokens[1].equals(tokens[4])) {
            Tuple2<String,String> strTuple2 = new Tuple2<String,String>(tokens[1], tokens[4]);
            Tuple2<String,String> strReverseTuple2 = new Tuple2<String,String>(tokens[4],
                    tokens[1]);

            if (proteinMap.containsKey(tokens[1])
                    && proteinMap.containsKey(tokens[4])) {
                if ((!vSeqSimMap.containsKey(strTuple2))
                        && (!vSeqSimMap.containsKey(strReverseTuple2))
                        && (1 == Integer.parseInt(tokens[10]))) {
                    vSeqSimMap.put(
                            strTuple2,
                            proteinMap.get(tokens[1])
                                    .createRelationshipTo(
                                            proteinMap.get(tokens[4]),
                                            EmbeddedGraph.RelTypes.SEQ_SIM));

                    vSeqSimMap
                            .get(strTuple2)
                            .setProperty(
                                    "Similarity_Score",
                                    Math.round(Double
                                            .parseDouble(tokens[7]) * 1000.0) / 1000.0);
                    vSeqSimMap.get(strTuple2).setProperty(
                            "Similarity_Significance", tokens[8]);
                    vSeqSimMap
                            .get(strTuple2)
                            .setProperty(
                                    "Alignment_Length",
                                    Math.round(Double
                                            .parseDouble(tokens[9]) * 1000.0) / 1000.0);
                }
            }
        }
    }

}
