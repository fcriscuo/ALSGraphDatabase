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
import org.eclipse.collections.impl.factory.Maps;
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
import org.nygenome.als.graphdb.value.RnaTpmGene;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import scala.collection.immutable.List;

public abstract class GraphDataConsumer implements Consumer<Path> {

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
  // RNA TPM Gene Cache
  private LoadingCache<RnaTpmGene,Node> rnaTpmGeneNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tuple2 -> GraphComponentFactory.INSTANCE.getRnaTpmGeneNodeFunction
          .apply(tuple2));

  // Disease cache
  private LoadingCache<String,Node> diseaseNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(diseaseId -> GraphComponentFactory.INSTANCE.getDiseaseNodeFunction
          .apply(diseaseId));

  // Pathway cache
  private LoadingCache<String,Node> pathwayNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(pathwayId -> GraphComponentFactory.INSTANCE.getPathwayNodeFunction
          .apply(pathwayId));
  // Tissue cache
  private LoadingCache<String,Node> tissueNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tissueId -> GraphComponentFactory.INSTANCE.getHumanTissueNodeFunction
          .apply(tissueId));

  // Subject cache
  private LoadingCache<String,Node> subjectNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(subjectId -> GraphComponentFactory.INSTANCE.getSubjectNodeFunction
          .apply(subjectId));

  // Sample cache

  private LoadingCache<String,Node> sampleNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(sampleId -> GraphComponentFactory.INSTANCE.getSampleNodeFunction
          .apply(sampleId));

  //SNP cache
  private LoadingCache<String,Node> snpNodeCache = Caffeine.newBuilder()
      .maximumSize(20_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(snpId -> GraphComponentFactory.INSTANCE.getSnpNodeFunction
          .apply(snpId));


  protected Map<Tuple2<String, String>, Relationship> proteinGeneOntologyRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinDiseaseRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinGeneticEntityMap =Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> geneticEntityDiseaseMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> alsWhiteListRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinDrugRelMap =Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> transcriptTissueMap= Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinProteinIntactMap = Maps.mutable.empty();
  protected Map<Tuple2<String,String>, Relationship> proteinPathwayMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> sequenceSimMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> subjectSampleRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinTPMRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinXrefRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinTissRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> snpDiseaseRelMap = Maps.mutable.empty();


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

  protected Function<String, Node> resolveSubjectNodeFunction = (extSubjectId) ->
      subjectNodeCache.get(extSubjectId);

  protected Function<String,Node> resolveHumanTissueNodeFunction  = (tissueId)->
      tissueNodeCache.get(tissueId);

  protected Function<String,Node> resolvePathwayNodeFunction = (pathwayId) ->
      pathwayNodeCache.get(pathwayId);


  protected Function<String,Node> resolveDiseaseNodeFunction = (diseaseId) ->
      diseaseNodeCache.get(diseaseId);

  /*
  Only this Consumer creates RnaTpmGene Nodes so it can be private and set the properties
   */
  protected Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) ->
        rnaTpmGeneNodeCache.get(rnaTpmGene);


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

  protected Function<String, Node> resolveEnsemblTranscriptNodeFunction =
      transcriptId ->
          xrefNodeCache.get(new Tuple2<>(transcriptId, LabelTypes.EnsemblTranscript));


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



  protected Function<String,Node> resolveSampleNodeByExternalIdFunction = (extSampleId)
      -> sampleNodeCache.get(extSampleId);


  protected Function<GeneOntology,Node> resolveGeneOntologyNodeFunction = (go) ->
      geneOntologyNodeCache.get(go);

protected Function<String,Node>  resolveSnpNodeFunction = (snpId) ->
    snpNodeCache.get(snpId);



}


