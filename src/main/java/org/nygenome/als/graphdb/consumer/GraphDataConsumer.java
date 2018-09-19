package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Strings;
import com.twitter.logging.Logger;

import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.DrugBankValue;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.HumanTissueAtlas;
import org.nygenome.als.graphdb.value.UniProtDrug;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import scala.collection.immutable.List;

public abstract class GraphDataConsumer implements Consumer<Path> {

  //private static final Logger log = Logger.getLogger(GraphDataConsumer.class);
  protected final String TAB_DELIM = "\t";  // tab delimited file
  protected final String COMMA_DELIM = ",";  // comma delimited file
  protected final String strNoInfo = "NA";
  protected final String strApostrophe = "`";
  protected final String HUMAN_SPECIES = "homo sapiens";
  protected Logger log = Logger.get(GraphDataConsumer.class);

  public enum EnsemblType_e {
    ENST, ENSG
  }

  public enum Resource_t {
    eKaneko, eDisGeNet, ePPI
  }

  protected Predicate<PathwayInfoConsumer.PathwayRecord> homoSapiensPredicate = (record) ->
      record.getSpecies().equalsIgnoreCase(HUMAN_SPECIES);

  protected Map<String, Node> proteinMap = new HashMap<String, Node>();
  protected Map<String, Node> geneOntologyMap = new HashMap<>();
  protected Map<String, Node> xrefMap = new HashMap<>();
  protected Map<String, Node> rnaTpmGeneMap = new HashMap<>();
  protected Map<String, Node> diseaseMap = new HashMap<String, Node>();
  protected Map<String, Node> drugMap = new HashMap<String, Node>();
  protected Map<String, Node> pathwayMap = new HashMap<String, Node>();
  protected Map<String, Node> tissueMap = new HashMap<String, Node>();
  protected Map<String, Node> GEOStudyMap = new HashMap<String, Node>();
  protected Map<String, Node> subjectMap = new HashMap<>();
  protected Map<String, Node> sampleMap = new HashMap<>();


  protected Map<Tuple2<String, String>, Node> GEOComparisonMap = new HashMap<Tuple2<String, String>, Node>();
  // Protein - Gene Ontology Relationships
  protected Map<Tuple2<String, String>, Relationship> proteinGeneOntologyRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinDiseaseRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> alsWhiteListRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinDrugRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vTissueRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinProteinIntactMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vPathwayMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vSeqSimMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> subjectSampleRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinTPMRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinXrefRelMap = new HashMap<>();


  /*
  Protected method to create two (2) directional relationships between two (2)
  specified nodes. The first relationship is registered in a Map to prevent
  duplication
  The relationship types are also provided
   */
  protected void createBiDirectionalRelationship(Node nodeA, Node nodeB,
      Tuple2<String, String> keyTuple,
      Map<Tuple2<String, String>, Relationship> relMap, RelTypes relTypeA, RelTypes relTypeB) {
    if (!relMap.containsKey(keyTuple)) {
      relMap.put(keyTuple, nodeA.createRelationshipTo(nodeB, relTypeA));
      nodeB.createRelationshipTo(nodeA, relTypeB);
      AsyncLoggingService.logInfo("Created realtionship between " + keyTuple._1() + " and "
          + keyTuple._2());
    }
  }

  /*
  Protected BiConsumer that will add a property name/value pair to a specified node
  Currently only String property values are supported
   */
  protected BiConsumer<Node, Tuple2<String, String>> nodePropertyValueConsumer = (node, propertyTuple) -> {
    if (!Strings.isNullOrEmpty(propertyTuple._2())) {
      node.setProperty(propertyTuple._1(), propertyTuple._2());
    }
  };

  /*
  Protected BiConsumer to register a List of property values for a specified node
  Property values are persisted as Strings
   */
  protected BiConsumer<Node, Tuple2<String, List<String>>> nodePropertyValueListConsumer = (node, propertyListTuple) -> {
    if (propertyListTuple._2() != null && propertyListTuple._2().size() > 0) {
      node.setProperty(propertyListTuple._1(), propertyListTuple._2().head());
    }
  };

  /*
  Private Function that creates a new Xref node for a specified HUGO Gene Symbol
  A second label identifies the xref as a HUGO Gene Symbol
   */
  private Function<String, Node> createHugoGeneNodeFunction = (hugoId) -> {
    Node node = EmbeddedGraph.getGraphInstance().createNode(LabelTypes.Xref);
    node.addLabel(LabelTypes.HUGO);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("HugoGeneSymbol", hugoId));
    xrefMap.put(hugoId, node);
    return node;
  };

  /*
  Protected Function that resolves a HUGO xref by either finding an existing Node
  with a specified gene symbol or creating a new Node for that symbol
   */
  protected Function<String, Node> resolveHugoGeneNodeFunction = (hugoId) ->
      (xrefMap.containsKey(hugoId)) ? xrefMap.get(hugoId)
          : createHugoGeneNodeFunction.apply(hugoId);

  /*
  Private Function that creates a new xref Node for a specified ensembl gene id
   */
  private Function<String, Node> createEnsemblGeneNodeFunction = (ensemblGeneId) -> {
    Node node = EmbeddedGraph.getGraphInstance().createNode(LabelTypes.Xref);
    node.addLabel(LabelTypes.EnsemblGene);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("ensemblGeneId", ensemblGeneId));
    xrefMap.put(ensemblGeneId, node);
    return node;
  };

  /*
  Protected Function that resolves a ensembl Gene xref by either finding an
  existing Node with a specified gene id or by creating a new Node for that id
   */
  protected Function<String, Node> resolveEnsemblGeneNodeFunction = (ensemblGeneId) ->
      (xrefMap.containsKey(ensemblGeneId)) ? xrefMap.get(ensemblGeneId)
          : createEnsemblGeneNodeFunction.apply(ensemblGeneId);

  /*
  Private Function that creates a new Protein Node for a specified UniProt id
   */
  private Function<String, Node> createProteinFunctionNode = (uniprotId) -> {
    AsyncLoggingService.logInfo("createProteinNodeFunction invoked for uniprot protein id  " +
        uniprotId);
    Node node = EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Protein);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtId", uniprotId));
    proteinMap.put(uniprotId, node);
    return node;
  };

  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      (proteinMap.containsKey(uniprotId)) ? proteinMap.get(uniprotId)
          : createProteinFunctionNode.apply(uniprotId);


  protected Consumer<UniProtValue> uniProtValueToProteinNodeConsumer = (upv) -> {
    Node node = resolveProteinNodeFunction.apply(upv.uniprotId());
    nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtName", upv.uniprotName()));
    nodePropertyValueListConsumer.accept(node, new Tuple2<>("ProteinName", upv.proteinNameList()));
    nodePropertyValueListConsumer.accept(node, new Tuple2<>("GeneSymbol", upv.geneNameList()));
  };

  /*
  Private Function to create a new DrugBank node in the graph and register it
  in the Map
   */
  protected Function<String, Node> resolveDrugBankNode = (dbId) -> {
    if (!drugMap.containsKey(dbId)) {
      AsyncLoggingService.logInfo("createDrugBankNode invoked for DrunkBank id  " +
          dbId);
      drugMap.put(dbId, EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Drug));
    }
    return drugMap.get(dbId);
  };

  /*
  Protected method to create a new DrugBank node
  The protein-drug relationships are created by processing different value objects
  which provide the type of relationship
   */
  protected void createDrugBankNode(String uniprotId, DrugBankValue dbv) {
    String key = dbv.drugBankId();
    if (!drugMap.containsKey(key)) {
      Node node = resolveDrugBankNode.apply(key);
      nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugId", dbv.drugBankId()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugName", dbv.drugName()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugType", dbv.drugType()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("CASNumber", dbv.casNumber()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("RxListLink", dbv.rxListLink()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("NDCLink", dbv.ndcLink()));
    }
  }

  /*

   */
  protected BiConsumer<RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
      = (drugRelType, drug) -> {
    String uniprotId = drug.uniprotId();
    // check if the protein node exists
    if (proteinMap.containsKey(uniprotId)) {
      Node proteinNode = proteinMap.get(uniprotId);
      drug.drugIdList().forEach((id) -> {
        Node drugNode = (drugMap.containsKey(id)) ? drugMap.get(id)
            : resolveDrugBankNode.apply(id);
        proteinDrugRelMap.put(new Tuple2<>(uniprotId, id),
            proteinNode.createRelationshipTo(drugNode, drugRelType));
      });
    } else {
      AsyncLoggingService.logError("resolveProteinDrugRelationship: "
          + " uniprot id: " + uniprotId + " is not registered.");
    }
  };

  protected void resolveProteinDrugRelationship(@Nonnull String uniprotId,
      @Nonnull RelTypes drugRelType, java.util.List<String> drugBankIdList) {
    // check if the protein node exists
    if (proteinMap.containsKey(uniprotId)) {
      Node proteinNode = proteinMap.get(uniprotId);
      drugBankIdList.forEach((id) -> {
        Node drugNode = (drugMap.containsKey(id)) ? drugMap.get(id)
            : resolveDrugBankNode.apply(id);
        proteinDrugRelMap.put(new Tuple2<>(uniprotId, id),
            proteinNode.createRelationshipTo(drugNode, drugRelType));
      });

    } else {
      AsyncLoggingService.logError("resolveProteinDrugRelationship: "
          + " uniprot id: " + uniprotId + " is not registered.");
    }
  }

  private Function<String, Node> createEnsemblTranscriptNodeFunction =
      (transcriptId) -> {
        xrefMap.put(transcriptId, EmbeddedGraph.getGraphInstance()
            .createNode(LabelTypes.Xref));
        Node node = xrefMap.get(transcriptId);
        node.addLabel(LabelTypes.EnsemblTranscript);
        nodePropertyValueConsumer.accept(node, new Tuple2<>("EnsemblTranscriptId", transcriptId));
        return node;
      };

  protected Function<String, Node> resolveEnsemblTranscriptNodeFunction =
      transcriptId ->
          (xrefMap.containsKey(transcriptId)) ?
              xrefMap.get(transcriptId)
              : createEnsemblTranscriptNodeFunction.apply(transcriptId);


  protected void createEnsemblTranscriptNodes(@Nonnull UniProtValue upv) {
    String uniprotId = upv.uniprotId();
    StringUtils.convertToJavaString(upv.ensemblTranscriptList()).forEach(transcriptId -> {
      Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(transcriptId);
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      Tuple2<String, String> key = new Tuple2<>(uniprotId, transcriptId);
      createBiDirectionalRelationship(proteinNode, transcriptNode, key,
          proteinXrefRelMap, RelTypes.MAPS_TO, RelTypes.MAPS_TO
      );
    });
  }

  /*
  Protected method to reolve a Sample node
  Will create a new one if necessary
   */
  protected Node resolveSampleNodeByExternalId(@Nonnull String externalSampleId) {
    if (!sampleMap.containsKey(externalSampleId)) {
      AsyncLoggingService.logInfo("creating Sample Node for external sample id  " +
          externalSampleId);
      return EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Sample);
    }
    return sampleMap.get(externalSampleId);
  }

  protected void createGeneOntologyNode(String uniprotId, GeneOntology go) {
    if (!geneOntologyMap.containsKey(go.goId())) {
      geneOntologyMap.put(go.goId(), EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.GeneOntology));
      Node node = geneOntologyMap.get(go.goId());
      nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyId", go.goId()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyAspect", go.goAspect()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyName", go.goName()));
    }
    // establish relationship to the protein node
    Tuple2<String, String> relKey = new Tuple2<>(uniprotId, go.goId());
    if (!proteinGeneOntologyRelMap.containsKey(relKey)) {
      Node proteinNode = proteinMap.get(uniprotId);
      Node goNode = geneOntologyMap.get(go.goId());
      proteinGeneOntologyRelMap.put(relKey,
          proteinNode.createRelationshipTo(goNode, RelTypes.GO_CLASSIFICATION)
      );
      AsyncLoggingService.logInfo("Created relationship beteen protein " + uniprotId
          + " and GO id: " + go.goId());
    }
  }

  protected void createProteinNode(String strProteinId, String szUniprotId,
      String szEnsembleTranscript, String szProteinName,
      String szGeneSymbol, String szEnsembl) {
    log.info("createProteinNode invoked for uniprot protein id  " + szUniprotId);

    proteinMap.put(szUniprotId, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.Protein));
    proteinMap.get(szUniprotId).setProperty("ProteinId", strProteinId);
    proteinMap.get(szUniprotId).setProperty("UniprotId", szUniprotId);
    proteinMap.get(szUniprotId).setProperty("EnsemblTranscript",
        szEnsembleTranscript);
    proteinMap.get(szUniprotId).setProperty("EnsemblId", szEnsembl);
    proteinMap.get(szUniprotId).setProperty("ProteinName", szProteinName);
    proteinMap.get(szUniprotId).setProperty("GeneSymbol", szGeneSymbol);
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


  protected void createTissueNode(HumanTissueAtlas ht) {
    String tissueCellType = ht.resolveTissueCellTypeLabel();
    tissueMap.put(tissueCellType, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.Tissue));
    tissueMap.get(tissueCellType).setProperty("TissueName", ht.tissue());
    tissueMap.get(tissueCellType).setProperty("CellType", ht.cellType());
  }


  protected void createGEOStudyNode(String szGEOStudyName) {
    GEOStudyMap.put(szGEOStudyName, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.GEOStudy));
    GEOStudyMap.get(szGEOStudyName).setProperty("GEOStudyID",
        szGEOStudyName);
  }

  protected void createGEOComparisonNode(Tuple2<String, String> szTuple) {
    GEOComparisonMap.put(szTuple, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.GEOComparison));
    GEOComparisonMap.get(szTuple).setProperty("GEOComparisonID",
        szTuple._1());
  }

  //createEnsemblTissueAssociation

  protected void createEnsemblTissueAssociation(@Nonnull HumanTissueAtlas ht) {
    // resolve the protein node
    if (!proteinMap.containsKey(ht.uniprotId())) {
      createProteinNode(strNoInfo, ht.uniprotId(), ht.ensemblTranscriptId(),
          strNoInfo, ht.geneName(), ht.ensemblGeneId());
    }
    if (!tissueMap.containsKey(ht.resolveTissueCellTypeLabel())) {
      createTissueNode(ht);
    }
    // create protein - tissue relationship

  }

  protected void createEnsemblTissueAssociation(String[] tokens) {
    String[] szTissueTokens = tokens[2].split("[:;]");

    for (Map.Entry<String, Node> eProtein : proteinMap.entrySet()) {
      if (eProtein.getValue().getProperty("EnsemblTranscript")
          .equals(tokens[1])) {
        eProtein.getValue()
            .setProperty("GeneSymbol", tokens[0]);

        for (int i = 0; i < szTissueTokens.length; i = i + 2) {
          for (Map.Entry<String, Node> eTissue : tissueMap
              .entrySet()) {
            if (eTissue.getKey().equals(szTissueTokens[i])) {
              Tuple2<String, String> strTuple2 = new Tuple2<>(
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


