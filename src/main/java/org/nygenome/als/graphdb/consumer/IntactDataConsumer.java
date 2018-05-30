package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.Utils;
import scala.Tuple2;

import java.nio.file.Path;

public class IntactDataConsumer extends GraphDataConsumer{
    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map(String::toUpperCase)
                .map((line -> line.split(TAB_DELIM)))
                .forEach(this::processIntactData);
    }
    
    private void processIntactData(String[] tokens){
        if (!tokens[0].equals(tokens[1])) {
            Tuple2<String,String> strTuple2 = new Tuple2<>(tokens[0], tokens[1]);
            Tuple2<String,String> strReverseTuple2 = new Tuple2<>(tokens[1],
                    tokens[0]);
            if ((!vPPIMap.containsKey(strTuple2))
                    && (!vPPIMap.containsKey(strReverseTuple2))) {
                if (!proteintMap.containsKey(tokens[0])) {
                    createProteinNode(strNoInfo, tokens[0], strNoInfo,
                            strNoInfo, strNoInfo, strNoInfo);
                }
                if (!proteintMap.containsKey(tokens[1])) {
                    createProteinNode(strNoInfo, tokens[1], strNoInfo,
                            strNoInfo, strNoInfo, strNoInfo);
                }
                vPPIMap.put(
                        strTuple2,
                        proteintMap
                                .get(tokens[0])
                                .createRelationshipTo(
                                        proteintMap.get(tokens[1]),
                                        Utils.convertStringToRelType(tokens[5])));
                vPPIMap.get(strTuple2).setProperty(
                        "Interaction_method_detection", tokens[2]);
                vPPIMap.get(strTuple2)
                        .setProperty("Reference", tokens[3]);
                vPPIMap.get(strTuple2).setProperty(
                        "Publication_identifier", tokens[4]);

                int len = tokens[6].length();
                int istart = 0;
                if (len >= 4) {
                    istart = len - 4;
                } else {
                    istart = 0;
                }
                String strToken = tokens[6].substring(istart, len);

                if (strToken.contains("\"")) {
                    strToken = strToken.replace("\"", "");
                }
                if (strToken.startsWith(".")) {
                    strToken = "0" + strToken;
                }
                vPPIMap.get(strTuple2).setProperty("Confidence_level",
                        Double.parseDouble(strToken));
            }
        }
    }

}

