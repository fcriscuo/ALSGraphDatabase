package org.nygenome.als.graphdb.consumer;


import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.eclipse.collections.impl.factory.Maps;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.DynamicLabel;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.RnaTpmGene;
import org.nygenome.als.graphdb.value.SampleVariantSummary;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;

public abstract class GraphDataConsumer implements Consumer<Path> {

  protected final String TAB_DELIM = "\t";  // tab delimited file
  protected final String COMMA_DELIM = ",";  // comma delimited file
  protected final String strNoInfo = "NA";
  protected final String strApostrophe = "`";
  protected final String HUMAN_SPECIES = "homo sapiens";
  protected Logger log = Logger.get(GraphDataConsumer.class);
  protected final Label alsLabel = new DynamicLabel("ALS-associated");

  public enum EnsemblType_e {
    ENST, ENSG
  }

  public enum Resource_t {
    eKaneko, eDisGeNet, ePPI
  }

  protected final FunctionLib lib = new FunctionLib();


  protected Map<Tuple2<String, String>, Relationship> proteinGeneOntologyRelMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> proteinDiseaseRelMap = Maps.mutable.empty();
  protected Map<Tuple2<String, String>, Relationship> proteinGeneticEntityMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> geneTranscriptMap = Maps.mutable
      .empty();
  protected Map<Tuple2<String, String>, Relationship> transcriptSnpMap = Maps.mutable
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

  private final Label subjectLabel  = new DynamicLabel("Subject");
  private final Label sampleLabel  = new DynamicLabel("Sample");
  private final Label tissueLabel = new DynamicLabel("Tissue");
  private final Label pathwayLabel = new DynamicLabel("Pathway");
  private final Label diseaseLabel = new DynamicLabel("Disease");
  private final Label rnaTpmLabel = new DynamicLabel("RnaTpm");
  private final Label geneticEntityLabel = new DynamicLabel("GeneticEntity");
  private final Label proteinLabel  = new DynamicLabel("Protein");
  private final Label drugBankLabel = new DynamicLabel("DrugBank");
  private final Label transcriptLabel = new DynamicLabel("EnsemblTranscript");
  private final Label geneLabel = new DynamicLabel("EnsemblGene");
  private final Label geneOntologyLabel = new DynamicLabel("GeneOntology");
  private final Label sampleVariantLabel = new DynamicLabel("SampleVariant");
  private final Label snpLabel = new DynamicLabel("SNP");


  protected Function<GeneOntology,Node> resolveGeneOntologyNodeFunction = (go)-> {
    Node goNode = lib.resolveNodeFunction.apply(geneOntologyLabel,"GeneOntology",go.goId());
    lib.novelLabelConsumer.accept(goNode, new DynamicLabel(go.goAspect()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyId", go.goId()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyPrinciple",
        go.goAspect()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyName", go.goName()));
    return goNode;
  };

  protected Function<String, Node> resolveSubjectNodeFunction =
      (extSubjectId) ->
     lib.resolveNodeFunction.apply( subjectLabel,
           "SubjectId",extSubjectId);

  protected Function<String, Node> resolveHumanTissueNodeFunction = (tissueId) ->
      lib.resolveNodeFunction.apply(tissueLabel,
          "TissueId",tissueId);

  protected Function<String, Node> resolvePathwayNodeFunction = (pathwayId) ->
      lib.resolveNodeFunction.apply(pathwayLabel,
          "PathwayId",pathwayId);


  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      lib.resolveNodeFunction.apply(diseaseLabel,
          "DiseaseId",diseaseId);


  protected Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) -> {
    Node node = lib.resolveNodeFunction.apply(rnaTpmLabel,
        "RnaTpmId", rnaTpmGene.id());
    if (node != null ) {
      // persist tpm value as a String
      lib.nodePropertyValueConsumer
          .accept(node, new Tuple2<>("TPM", String.valueOf(rnaTpmGene.tpm())));
    }
    return node;
  };


  protected Function<String,Node> resolveSampleNodeFunction = (sampleId) ->
      lib.resolveNodeFunction.apply(sampleLabel, "SampleId", sampleId);


  private Function<String, Node> resolveGeneticEntityNodeFunction = (geneticEntityId) ->
     lib.resolveNodeFunction.apply(geneticEntityLabel,"GeneticEntityId", geneticEntityId);

  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      lib.resolveNodeFunction.apply(proteinLabel,"UniProtKBID", uniprotId);


  protected Function<String, Node> resolveDrugBankNode = (dbId) ->
      lib.resolveNodeFunction.apply(drugBankLabel,"DrugBankId", dbId);

  protected Function<String, Node> resolveEnsemblTranscriptNodeFunction =
      transcriptId -> {
        Node node = resolveGeneticEntityNodeFunction.apply(transcriptId);
        lib.novelLabelConsumer.accept(node,transcriptLabel);
        return node;
      };

  protected Function<String, Node> resolveEnsemblGeneNodeFunction =
      geneId -> {
        Node node = resolveGeneticEntityNodeFunction.apply(geneId);
        lib.novelLabelConsumer.accept(node,geneLabel);
        return node;
      };

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
protected Function<SampleVariantSummary,Node> resolveSampleVariantNode = (svc) -> {

  Node svNode =lib.resolveNodeFunction.apply(sampleVariantLabel,"SampleVariantId", svc.id());
  // TODO: add test for ALS gene and add label if so
  // persist the list of variants
  lib.nodePropertyValueStringArrayConsumer.accept(svNode,new Tuple2<>("Variants", svc.variantList()));
  return svNode;
};

  protected Function<String, Node> resolveSnpNodeFunction = (snpId) ->
      lib.resolveNodeFunction.apply(snpLabel,"SNP",snpId);


}


