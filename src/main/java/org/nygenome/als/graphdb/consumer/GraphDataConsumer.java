package org.nygenome.als.graphdb.consumer;



import com.twitter.logging.Logger;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.EmbeddedGraph;
import scala.Tuple2;
import scala.Tuple3;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class GraphDataConsumer   implements Consumer<Path> {
    //private static final Logger log = Logger.getLogger(GraphDataConsumer.class);
    protected final String TAB_DELIM = "\t";  // tab delimited file
    protected final String COMMA_DELIM = ",";  // comma delimited file
    protected final String strNoInfo = "NA";
    protected final String strApostrophe = "`";
    protected final String HUMAN_SPECIES = "homo sapiens";
    protected Logger log = Logger.get(GraphDataConsumer.class);

    public  enum EnsemblType_e {
        ENST, ENSG
    }

    public enum Resource_t {
        eKaneko, eDisGeNet, ePPI
    }

    protected Predicate<PathwayInfoConsumer.PathwayRecord> homoSapiensPredicate = (record) ->
        record.getSpecies().equalsIgnoreCase(HUMAN_SPECIES);

    protected Map<String, Node> proteintMap = new HashMap<String, Node>();
    protected Map<String, Node> diseaseMap = new HashMap<String, Node>();
    protected Map<String, Node> drugMap = new HashMap<String, Node>();
    protected Map<String, Node> pathwayMap = new HashMap<String, Node>();
    protected Map<String, Node> tissueMap = new HashMap<String, Node>();
    protected Map<String, Node> GEOStudyMap = new HashMap<String, Node>();
    protected Map<Tuple2<String,String>, Node> GEOComparisonMap = new HashMap<Tuple2<String,String>, Node>();
    protected Map<Tuple2<String,String>, Relationship> vDiseaseRelMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vKanekoRelMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vDrugRelMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vTissueRelMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vPPIMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vPathwayMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple3<String,String,String>, Relationship> vGEOGeneRelMap = new HashMap<Tuple3<String,String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vGEOComponentsRelMap = new HashMap<Tuple2<String,String>, Relationship>();
    protected Map<Tuple2<String,String>, Relationship> vSeqSimMap = new HashMap<Tuple2<String,String>, Relationship>();



    protected void createProteinNode(String strProteinId, String szUniprotId,
                                   String szEnsembleTranscript, String szProteinName,
                                   String szGeneSymbol, String szEnsembl) {
        log.ifInfo(() -> "createProteinNode invoked for uniprot protein id  " +szUniprotId);

        proteintMap.put(szUniprotId, EmbeddedGraph.getGraphInstance()
                .createNode(EmbeddedGraph.LabelTypes.Protein));
        proteintMap.get(szUniprotId).setProperty("ProteinId", strProteinId);
        proteintMap.get(szUniprotId).setProperty("UniprotId", szUniprotId);
        proteintMap.get(szUniprotId).setProperty("EnsemblTranscript",
                szEnsembleTranscript);
        proteintMap.get(szUniprotId).setProperty("EnsemblId", szEnsembl);
        proteintMap.get(szUniprotId).setProperty("ProteinName", szProteinName);
        proteintMap.get(szUniprotId).setProperty("GeneSymbol", szGeneSymbol);
    }

    protected void createDiseaseNode(String szDiseaseName) {
        diseaseMap.put(szDiseaseName, EmbeddedGraph.getGraphInstance()
                .createNode(EmbeddedGraph.LabelTypes.Disease));
        diseaseMap.get(szDiseaseName).setProperty("DiseaseName", szDiseaseName);
    }

    protected void createDrugNode(String szDrugId, String szDrugName,
                                String szDrugType) {
        drugMap.put(szDrugId,
                EmbeddedGraph.getGraphInstance().createNode(EmbeddedGraph.LabelTypes.Drug));
        drugMap.get(szDrugId).setProperty("DrugId", szDrugId);
        drugMap.get(szDrugId).setProperty("DrugName", szDrugName);
        drugMap.get(szDrugId).setProperty("DrugType", szDrugType);
    }


    protected void createTissueNode(String szTissueName) {
        tissueMap.put(szTissueName, EmbeddedGraph.getGraphInstance()
                .createNode(EmbeddedGraph.LabelTypes.Tissue));
        tissueMap.get(szTissueName).setProperty("TissueName", szTissueName);
    }



    protected void createGEOStudyNode(String szGEOStudyName) {
        GEOStudyMap.put(szGEOStudyName, EmbeddedGraph.getGraphInstance()
                .createNode(EmbeddedGraph.LabelTypes.GEOStudy));
        GEOStudyMap.get(szGEOStudyName).setProperty("GEOStudyID",
                szGEOStudyName);
    }

    protected void createGEOComparisonNode(Tuple2<String,String> szTuple) {
        GEOComparisonMap.put(szTuple, EmbeddedGraph.getGraphInstance()
                .createNode(EmbeddedGraph.LabelTypes.GEOComparison));
        GEOComparisonMap.get(szTuple).setProperty("GEOComparisonID",
                szTuple._1());
    }

    //createEnsemblTissueAssociation
    protected void createEnsemblTissueAssociation(String[] tokens){
        String[] szTissueTokens = tokens[2].split("[:;]");

        for (Map.Entry<String, Node> eProtein : proteintMap.entrySet()) {
            if (eProtein.getValue().getProperty("EnsemblTranscript")
                    .equals(tokens[1])) {
                eProtein.getValue()
                        .setProperty("GeneSymbol", tokens[0]);

                for (int i = 0; i < szTissueTokens.length; i = i + 2) {
                    for (Map.Entry<String, Node> eTissue : tissueMap
                            .entrySet()) {
                        if (eTissue.getKey().equals(szTissueTokens[i])) {
                            Tuple2<String,String> strTuple2 = new Tuple2<>(
                                    tokens[1], eTissue.getKey());
                            if (!vTissueRelMap.containsKey(strTuple2)) {
                                vTissueRelMap
                                        .put(strTuple2,
                                                eProtein.getValue()
                                                        .createRelationshipTo(
                                                                eTissue.getValue(),
                                                                EmbeddedGraph.RelTypes.TISSUE_ENHANCED));
                                vTissueRelMap.get(strTuple2).setProperty(
                                        "RNA_TS_FPKM_value",
                                        szTissueTokens[i + 1]);
                            }
                            break;
                        }
                    }
                }
                break;
            }
        }
    }
}


