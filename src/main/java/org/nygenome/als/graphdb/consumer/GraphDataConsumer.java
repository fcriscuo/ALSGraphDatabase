package org.nygenome.als.graphdb.consumer;


import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.service.GraphComponentFactory;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.RnaTpmGene;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;

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

// Node caches
  // Use Caffeine API instead of Google Guava to avoid dealing
  // with concurrent access Exception

  // Protein Node cache
  private LoadingCache<String, Node> proteinNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(uniprotId -> GraphComponentFactory.INSTANCE.getProteinNodeFunction
          .apply(uniprotId));
  // GeneticEntity Node cache
  private LoadingCache<String, Node> geneticEntityNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(ensemblGeneId -> GraphComponentFactory.INSTANCE.getGeneticEntityNodeFunction
          .apply(ensemblGeneId));
  //DrugBank Node Cache
  private LoadingCache<String, Node> drugBankNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(drugBankId -> GraphComponentFactory.INSTANCE.getDrugBankNodeFunction
          .apply(drugBankId));
  // Gene Ontology Cache

  private LoadingCache<GeneOntology, Node> geneOntologyNodeCache = Caffeine.newBuilder()
      .maximumSize(7_000)
      .expireAfterWrite(5, TimeUnit.MINUTES)
      .build(goId -> GraphComponentFactory.INSTANCE.getGeneOntologyNodeFunction
          .apply(goId));

  // xref Node cache
  private LoadingCache<Tuple2<String, LabelTypes>, Node> xrefNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tuple2 -> GraphComponentFactory.INSTANCE.getXrefNodeFunction
          .apply(tuple2));
  // RNA TPM Gene Cache
  private LoadingCache<RnaTpmGene, Node> rnaTpmGeneNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tuple2 -> GraphComponentFactory.INSTANCE.getRnaTpmGeneNodeFunction
          .apply(tuple2));

  // Disease cache
  private LoadingCache<String, Node> diseaseNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(diseaseId -> GraphComponentFactory.INSTANCE.getDiseaseNodeFunction
          .apply(diseaseId));

  // Pathway cache
  private LoadingCache<String, Node> pathwayNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(pathwayId -> GraphComponentFactory.INSTANCE.getPathwayNodeFunction
          .apply(pathwayId));
  // Tissue cache
  private LoadingCache<String, Node> tissueNodeCache = Caffeine.newBuilder()
      .maximumSize(10_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(tissueId -> GraphComponentFactory.INSTANCE.getHumanTissueNodeFunction
          .apply(tissueId));

  // Subject cache
  private LoadingCache<String, Node> subjectNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(subjectId -> GraphComponentFactory.INSTANCE.getSubjectNodeFunction
          .apply(subjectId));

  // Sample cache

  private LoadingCache<String, Node> sampleNodeCache = Caffeine.newBuilder()
      .maximumSize(1_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(sampleId -> GraphComponentFactory.INSTANCE.getSampleNodeFunction
          .apply(sampleId));

  //SNP cache
  private LoadingCache<String, Node> snpNodeCache = Caffeine.newBuilder()
      .maximumSize(20_000)
      .expireAfterWrite(15, TimeUnit.MINUTES)
      .build(snpId -> GraphComponentFactory.INSTANCE.getSnpNodeFunction
          .apply(snpId));


  protected Map<Tuple2<String, String>, Relationship> proteinGeneOntologyRelMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> proteinDiseaseRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinGeneticEntityMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> geneTranscriptMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> geneticEntityDiseaseMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> alsWhiteListRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinDrugRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> transcriptTissueMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinProteinIntactMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> proteinPathwayMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> sequenceSimMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> subjectSampleRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinTPMRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinXrefRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinTissRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> snpDiseaseRelMap = Maps.mutable.empty();

  protected Function<String, Node> resolveSubjectNodeFunction = (extSubjectId) ->
      subjectNodeCache.get(extSubjectId);

  protected Function<String, Node> resolveHumanTissueNodeFunction = (tissueId) ->
      tissueNodeCache.get(tissueId);

  protected Function<String, Node> resolvePathwayNodeFunction = (pathwayId) ->
      pathwayNodeCache.get(pathwayId);


  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      diseaseNodeCache.get(diseaseId);

  /*
  Only this Consumer creates RnaTpmGene Nodes so it can be private and set the properties
   */
  protected Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) ->
      rnaTpmGeneNodeCache.get(rnaTpmGene);

  /*
  Protected Function that resolves a Gene by either finding an existing Node
  with a specified ensembl gene id or creating a new Node for that id
   */
  protected Function<String, Node> resolveGeneticEntityNodeFunction = (geneticEntityId) ->
      geneticEntityNodeCache.get(geneticEntityId);

  /*
  Protected Function that resolves a ensembl Gene xref by either finding an
  existing Node with a specified gene id or by creating a new Node for that id
   */
  protected Function<String, Node> resolveEnsemblGeneNodeFunction = (ensemblGeneId) ->
      xrefNodeCache.get(new Tuple2<>(ensemblGeneId, LabelTypes.EnsemblGene));
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
      lib.createBiDirectionalRelationship(proteinNode, transcriptNode, key,
          proteinXrefRelMap, RelTypes.MAPS_TO, RelTypes.MAPS_TO
      );
    });
  }


  protected Function<String, Node> resolveSampleNodeByExternalIdFunction = (extSampleId)
      -> sampleNodeCache.get(extSampleId);


  protected Function<GeneOntology, Node> resolveGeneOntologyNodeFunction = (go) ->
      geneOntologyNodeCache.get(go);

  protected Function<String, Node> resolveSnpNodeFunction = (snpId) ->
      snpNodeCache.get(snpId);


}


