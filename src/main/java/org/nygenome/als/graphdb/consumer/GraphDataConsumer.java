package org.nygenome.als.graphdb.consumer;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.base.Strings;

import com.twitter.logging.Logger;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.service.GraphComponentFactory;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.HumanTissueAtlas;
import org.nygenome.als.graphdb.value.Pathway;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

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

protected final FunctionLib lib = new FunctionLib();

// Node and Relationship caches
  // Use Caffeine API instead of Google Guava to avoid dealing
  // with concurrent Exception
  // Protein Node cache
private LoadingCache<String,Node> proteinNodeCache = Caffeine.newBuilder()
    .maximumSize(10_000)
    .expireAfterWrite(5, TimeUnit.MINUTES)
    .build(uniprotId -> GraphComponentFactory.INSTANCE.getProteinNodeFunction
        .apply(uniprotId));
  // Gene Node cache
  private LoadingCache<String,Node> geneNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(hugoId -> GraphComponentFactory.INSTANCE.getGeneNodeFunction
          .apply(hugoId));
  //DrugBank Node Cache
  private LoadingCache<String,Node> drugBankNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(drugBankId -> GraphComponentFactory.INSTANCE.getDrugBankNodeFunction
          .apply(drugBankId));
  // Gene Ontology Cache

  private LoadingCache<GeneOntology,Node> geneOntologyNodeCache = Caffeine.newBuilder()
      .maximumSize(7_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(goId -> GraphComponentFactory.INSTANCE.getGeneOntologyNodeFunction
          .apply(goId));

  // xref Node cache
  private LoadingCache<Tuple2<String,LabelTypes>,Node> xrefNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tuple2 -> GraphComponentFactory.INSTANCE.getXrefNodeFunction
          .apply(tuple2));

  protected Map<String, Node> geneOntologyMap = new HashMap<>();
  protected Map<String, Node> xrefMap = new HashMap<>();
  protected Map<String, Node> rnaTpmGeneMap = new HashMap<>();
  protected Map<String, Node> diseaseMap = new HashMap<String, Node>();
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
  protected Map<Tuple2<String, String>, Relationship> geneticEntityDiseaseMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> alsWhiteListRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinDrugRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> vTissueRelMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> proteinProteinIntactMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<String, Relationship> proteinPathwayMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> sequenceSimMap = new HashMap<Tuple2<String, String>, Relationship>();
  protected Map<Tuple2<String, String>, Relationship> subjectSampleRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinTPMRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinXrefRelMap = new HashMap<>();
  protected Map<Tuple2<String, String>, Relationship> proteinTissRelMap = new HashMap<>();


  /*
  Protected method to create two (2) directional relationships between two (2)
  specified nodes. The first relationship is registered in a Map to prevent
  duplication
  The relationship types are also provided
  The created or existing Relationships are returned in a Tuple2 in the A->B, & B->A order
   */
  protected Tuple2<Relationship,Relationship> createBiDirectionalRelationship(Node nodeA, Node nodeB,
      Tuple2<String, String> keyTuple,
      Map<Tuple2<String, String>, Relationship> relMap, RelTypes relTypeA, RelTypes relTypeB) {
    if (!relMap.containsKey(keyTuple)) {
      Relationship relA = nodeA.createRelationshipTo(nodeB, relTypeA);
      relMap.put(keyTuple, relA);
      Relationship relB = nodeB.createRelationshipTo(nodeA, relTypeB);
      relMap.put(keyTuple.swap(), relB);
      AsyncLoggingService.logInfo("Created realtionship between " + keyTuple._1() + " and "
          + keyTuple._2());
    }
    return new Tuple2<>(relMap.get(keyTuple),relMap.get(keyTuple.swap()));
  }
  /*
  Protected BiConsumer that accepts a Pair of Realtionships and a property key/value pair
  The supplied property is applied to each of the Relationships
   */
  protected BiConsumer<Tuple2<Relationship,Relationship> , Tuple2<String,String>> relationshipPairPropertyConsumer
      = (relPair,keyValue) -> {
    relPair._1().setProperty(keyValue._1(), keyValue._2());
    relPair._2().setProperty(keyValue._1(), keyValue._2());
  };

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
  Protected Function that resolves a Gene by either finding an existing Node
  with a specified gene symbol or creating a new Node for that symbol
   */
  protected Function<String, Node> resolveGeneNodeFunction = (hugoId) ->
     geneNodeCache.get(hugoId);

  /*
  Protected Function that resolves a ensembl Gene xref by either finding an
  existing Node with a specified gene id or by creating a new Node for that id
   */
  protected Function<String, Node> resolveEnsemblGeneNodeFunction = (ensemblGeneId) ->
      xrefNodeCache.get(new Tuple2<>(ensemblGeneId,LabelTypes.EnsemblGene));
//      (xrefMap.containsKey(ensemblGeneId)) ? xrefMap.get(ensemblGeneId)
//          : createEnsemblGeneNodeFunction.apply(ensemblGeneId);


  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      proteinNodeCache.get(uniprotId);

  /*
  Protected Consumer that will create a Protein Node with properties or
  add properties to an existing Protein Node
   */


  protected Function<String, Node> resolveDrugBankNode = (dbId) ->
      drugBankNodeCache.get(dbId);





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



  protected Function<GeneOntology,Node> resolveGeneOntologyNodeFunction = (go) ->
      geneOntologyNodeCache.get(go);


  private Function<Pathway, Optional<Node>> createPathwayNodeFunction = (pathway) -> {
    Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get();
    try {
      Node node = EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Pathway);
      pathwayMap.put(pathway.reactomeId(), node);
      nodePropertyValueConsumer.accept(node, new Tuple2<>("ReactomeId", pathway.reactomeId()));
      nodePropertyValueConsumer.accept(node, new Tuple2<>("Pathway", pathway.eventName()));
      AsyncLoggingService.logInfo("creatPathway Node for Reactome ID: " + pathway.reactomeId());
      tx.success();
      return Optional.of(node);
    } catch (Exception e) {
      AsyncLoggingService.logError("ERR: createPathwayNodeFunction " + e.getMessage());
      tx.failure();
    } finally {
      tx.close();
    }
    return Optional.empty();
  };

  /*
  Protected Function to either find an existing Pathway Node or create a
  new one
   */
  protected Function<Pathway, Optional<Node>> resolvePathwayNode = (pathway) ->
      (pathwayMap.containsKey(pathway.reactomeId())) ? Optional.of(pathwayMap.get(pathway.reactomeId()))
          : createPathwayNodeFunction.apply(pathway);

  protected void createDiseaseNode(String szDiseaseName) {
    diseaseMap.put(szDiseaseName, EmbeddedGraph.getGraphInstance()
        .createNode(EmbeddedGraph.LabelTypes.Disease));
    diseaseMap.get(szDiseaseName).setProperty("DiseaseName", szDiseaseName);
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
  private Function<String, Node> createDiseaseNodeFunction = (diseaseId) -> {
    AsyncLoggingService.logInfo("createDiseasekNode invoked for Disease id  " +
        diseaseId);
    Node diseaseNode = EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Disease);
    nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseId", diseaseId));
    diseaseMap.put(diseaseId, diseaseNode);
    return diseaseNode;
  };

  /*
  Protected Function to find an existing or create a new Disease Node based on its id
   */
  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      (diseaseMap.containsKey(diseaseId)) ? diseaseMap.get(diseaseId)
          : createDiseaseNodeFunction.apply(diseaseId);

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
  private Function<HumanTissueAtlas, Node> createHumanTissueNodeFunction = (ht) -> {
    Node tissueNode = EmbeddedGraph.getGraphInstance()
        .createNode(LabelTypes.Tissue);

    tissueMap.put(ht.resolveTissueCellTypeLabel(), tissueNode);
    AsyncLoggingService.logInfo("createHumanTissueNodeFunction invoked for "
        + ht.resolveTissueCellTypeLabel());
    return tissueNode;
  };

  /*
  Protected Function to retrieve an exisiting HumanTissue Node by its
  tissue+cell label ore create a new one
   */
  protected Function<HumanTissueAtlas, Node> resolveHumanTissueAtlasNodeFunction =
      (ht) -> (tissueMap.containsKey(ht.resolveTissueCellTypeLabel())) ?
          tissueMap.get(ht.resolveTissueCellTypeLabel())
          : createHumanTissueNodeFunction.apply(ht);


}


