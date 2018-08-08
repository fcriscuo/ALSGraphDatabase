package org.nygenome.als.graphdb.consumer;

import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.model.PsiMitab;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.Utils;
import scala.Tuple2;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class IntactDataConsumer extends GraphDataConsumer implements Consumer<Path> {

    private Predicate<PsiMitab> selfInteractionPredicate = (ppi) ->
       ! ppi.getIntearctorAId().equals(ppi.getInteractorBId());
    @Override
    public void accept(Path path) {
        new TsvRecordStreamSupplier(path).get()
            .map(PsiMitab.parseCsvRecordFunction)
            .filter(selfInteractionPredicate)
            .forEach(proteinInteractionCconsumer);
    }

    private Consumer<Tuple2<String,String>> novelProteinNodesConsumer = (tuple) -> {
        if (!proteintMap.containsKey(tuple._1())) {
            createProteinNode(strNoInfo, tuple._1(), strNoInfo,
                strNoInfo, strNoInfo, strNoInfo);
        }
        if (!proteintMap.containsKey(tuple._2())) {
            createProteinNode(strNoInfo,tuple._2(), strNoInfo,
                strNoInfo, strNoInfo, strNoInfo);
        }
    };
/*
#ID(s) interactor A
ID(s) interactor B
Alt. ID(s) interactor A
Alt. ID(s) interactor B
	Alias(es) interactor A
	Alias(es) interactor B
	Interaction detection method(s)
	Publication 1st author(s)
	Publication Identifier(s)
	Taxid interactor A
	Taxid interactor B
	nteraction type(s)
	Source database(s)
	Interaction identifier(s)
	Confidence value(s)
	Expansion method(s)
	Biological role(s) interactor A	Biological role(s) interactor B	Experimental role(s) interactor A	Experimental role(s) interactor B	Type(s) interactor A	Type(s) interactor B	Xref(s) interactor A	Xref(s) interactor B	Interaction Xref(s)	Annotation(s) interactor A	Annotation(s) interactor B	Interaction annotation(s)	Host organism(s)	Interaction parameter(s)	Creation date	Update date	Checksum(s) interactor A	Checksum(s) interactor B	Interaction Checksum(s)	Negative	Feature(s) interactor A	Feature(s) interactor B	Stoichiometry(s) interactor A	Stoichiometry(s) interactor B	Identification method participant A	Identification method participant B
uniprotkb:O54918-3
 */

    private Consumer<PsiMitab> proteinInteractionCconsumer = (ppi) -> {
        Tuple2<String,String> abTuple = new Tuple2<>(ppi.getIntearctorAId(),ppi.getInteractorBId());
        Tuple2<String,String> baTuple = new Tuple2<>(ppi.getInteractorBId(), ppi.getIntearctorAId());
        if ((!vPPIMap.containsKey(abTuple))
            && (!vPPIMap.containsKey(baTuple))) {
             novelProteinNodesConsumer.accept(abTuple);  // register protein node if novel
            vPPIMap.put(
               abTuple,
                proteintMap
                    .get(ppi.getIntearctorAId())
                    .createRelationshipTo(
                        proteintMap.get(ppi.getInteractorBId()),
                        Utils.convertStringToRelType("A")));
        }



    };

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

