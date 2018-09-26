package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Strings;
import com.twitter.logging.Logger;

import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.DrugBankValue;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.HumanTissueAtlas;
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
  protected Map<String,Node> geneticEntityMap = new HashMap<>();
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
  protected Map<Tuple2<String, String>, Relationship> proteinGeneticEntityMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String,String>,Relationship> geneticEntityDiseaseMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> alsWhiteListRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinDrugRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vTissueRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinProteinIntactMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vPathwayMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vSeqSimMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> subjectSampleRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinTPMRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinXrefRelMap = new HashMap<>();
  protected Map<Tuple2<String,String> ,Relationship> proteinTissRelMap  = new HashMap<>();


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
  Private Function that creates a new GeneticEntity node for a specified HUGO Gene Symbol
  A second label identifies the genetic entity as a Gene
   */
  private Function<String, Node> createGeneNodeFunction = (hugoId) -> {
    Node node = EmbeddedGraph.getGraphInstance().createNode(LabelTypes.GeneticEntity);
    node.addLabel(LabelTypes.Gene);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneSymbol", hugoId));
    geneticEntityMap.put(hugoId, node);
    return node;
  };

  /*
  Protected Function that resolves a Gene by either finding an existing Node
  with a specified gene symbol or creating a new Node for that symbol
   */
  protected Function<String, Node> resolveGeneNodeFunction = (hugoId) ->
      (geneticEntityMap.containsKey(hugoId)) ? geneticEntityMap.get(hugoId)
          : createGeneNodeFunction.apply(hugoId);

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
    Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
    AsyncLoggingService.logInfo("createProteinNodeFunction invoked for uniprot protein id  " +
        uniprotId);
    try {
      Node node = EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Protein);
      nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtId", uniprotId));
      proteinMap.put(uniprotId, node);
      tx.success();
      return node;
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    } finally {
      tx.close();
    }
    return null;

  };

  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      (proteinMap.containsKey(uniprotId)) ? proteinMap.get(uniprotId)
          : createProteinFunctionNode.apply(uniprotId);

/*
Protected Consumer that will create a Protein Node with properties or
add properties to an existing Protein Node
 */
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
  private Function<String, Node> createDrugBankNodeFunction = (dbId) -> {
    AsyncLoggingService.logInfo("createDrugBankNode invoked for DrunkBank id  " +
        dbId);
    Node node =  EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Drug);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugBankId",
        dbId));
    drugMap.put(dbId, node);
    return node;
  };
  protected Function<String, Node> resolveDrugBankNode = (dbId) ->
      (drugMap.containsKey(dbId))? drugMap.get(dbId)
          : createDrugBankNodeFunction.apply(dbId);


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
  Protected method to resolve a Sample node
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


  /*
  Protected Function to select the correct Gene Ontology principle label
   */
  protected Function <String,LabelTypes> resolveGeneOntologyPrincipleFunction = (princ) ->{
        if(princ.toUpperCase().startsWith("MOLECULAR")) {
          return LabelTypes.MolecularFunction;
        }
    if(princ.toUpperCase().startsWith("BIOLOGICAL")) {
      return LabelTypes.BiologicalProcess;
    }
    if(princ.toUpperCase().startsWith("CELLULAR")) {
      return LabelTypes.CellularComponents;
    }
    AsyncLoggingService.logError(princ +" is not a valid Gene Ontology principle " );
    return LabelTypes.Unknown;

  };


  /*
  Private function to create a new GeneOntology node with an id and GO principle
   */
  private Function<GeneOntology,Node> createGeneOntologyNodeFunction = (go) -> {
      Node node = EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.GeneOntology);
      node.addLabel(resolveGeneOntologyPrincipleFunction.apply(go.goAspect()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyId",go.goId()));
     nodePropertyValueConsumer.accept(node,new Tuple2<>("GeneOntologyPrinciple",
        go.goAspect()));
    geneOntologyMap.put(go.goId(),node);
    nodePropertyValueConsumer.accept(node, new Tuple2<>("GeneOntologyName", go.goName()));
    return node;
  };

  protected Function<GeneOntology,Node> resolveGeneOntologyNodeFunction = (go)->
      (geneOntologyMap.containsKey(go.goId()))? geneOntologyMap.get(go.goId())
          : createGeneOntologyNodeFunction.apply(go);



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

  /*
  Private Function to create a new disease node
   */
  private Function<String,Node> createDiseaseNodeFunction = (diseaseId)-> {
    AsyncLoggingService.logInfo("createDiseasekNode invoked for Disease id  " +
        diseaseId);
    Node diseaseNode = EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Disease);
     nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseId",diseaseId));
    diseaseMap.put(diseaseId,diseaseNode);
    return diseaseNode;
  };

  /*
  Protected Function to find an existing or create a new Disease Node based on its id
   */
  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      (diseaseMap.containsKey(diseaseId))? diseaseMap.get(diseaseId)
          :createDiseaseNodeFunction.apply(diseaseId);

  protected void createGEOComparisonNode(Tuple2<String, String> szTuple) {
    GEOComparisonMap.put(szTuple, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.GEOComparison));
    GEOComparisonMap.get(szTuple).setProperty("GEOComparisonID",
        szTuple._1());
  }

  /*
  Private function to create a new HumanTissueAtlas Node from a HumanTissueAtlas
  value object
  Since a complete value object is used an a parameter, the nodes properties
  can be set a s well
   */
  private Function<HumanTissueAtlas, Node> createHumanTissueNodeFunction = (ht)-> {
    Node tissueNode = EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Tissue);
    
    tissueMap.put(ht.resolveTissueCellTypeLabel(), tissueNode);
    AsyncLoggingService.logInfo("createHumanTissueNodeFunction invoked for "
    +ht.resolveTissueCellTypeLabel());
    return tissueNode;
  };

  /*
  Protected Function to retrieve an exisiting HumanTissue Node by its
  tissue+cell label ore create a new one
   */
  protected Function<HumanTissueAtlas,Node> resolveHumanTissueAtlasNodeFunction =
      (ht)  -> (tissueMap.containsKey(ht.resolveTissueCellTypeLabel())) ?
          tissueMap.get(ht.resolveTissueCellTypeLabel())
          : createHumanTissueNodeFunction.apply(ht);





}


