package org.nygenome.als.graphdb.consumer;


import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import scala.Tuple2;
import java.nio.file.Path;
import java.util.Map;

/*
Consumer of gene disease association data from a specified file
Data mapped to data structures for entry into Neo4j database
 */

public class GeneDiseaseAssociationDataConsumer extends GraphDataConsumer {
    private static final Logger log = Logger.getLogger(GraphDataConsumer.class);

    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map((line -> line.split(TAB_DELIM)))
                .forEach(this::processGeneDiseaseAssociationData);
        log.info("The disease map has " + diseaseMap.size() +" entries");
    }


    private void processGeneDiseaseAssociationData(String[] tokens) {
                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }
                String szUniprotId = null;
                for (Map.Entry<String, Node> eProtein : proteintMap.entrySet()) {
                    if (eProtein.getValue().getProperty("ProteinId").toString()
                            .equals(tokens[0])) {
                        szUniprotId = eProtein.getKey();
                        proteintMap.get(szUniprotId).setProperty("GeneSymbol",
                                tokens[1]);
                        break;
                    }
                }
                if (null != szUniprotId) {
                    if (!diseaseMap.containsKey(tokens[4])) {
                        createDiseaseNode(tokens[4]);
                    }
                    Tuple2 strPair = new Tuple2(szUniprotId, tokens[4]);
                    if (!vDiseaseRelMap.containsKey(strPair)) {
                        vDiseaseRelMap
                                .put(strPair,
                                        proteintMap
                                                .get(szUniprotId)
                                                .createRelationshipTo(
                                                        diseaseMap
                                                                .get(tokens[4]),
                                                        Utils.convertStringToRelType(tokens[7])));
                        vDiseaseRelMap.get(strPair).setProperty(
                                "Confidence_level",
                                Double.parseDouble(tokens[5]));
                        vDiseaseRelMap.get(strPair).setProperty("Reference",
                                tokens[8]);
                    }
                }
            }
 // main method for stand alone testing
    public static void main(String... args) {
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_UNIPROT_ID_ASSOC_DISGENET_FILE")
                .ifPresent(new GeneUniprotIdAssociationDataConsumer());
        FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
                .ifPresent(new GeneUniprotIdAssociationDataConsumer());



    }

    }

