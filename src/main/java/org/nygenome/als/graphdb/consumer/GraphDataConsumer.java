package org.nygenome.als.graphdb.consumer;


import com.twitter.logging.Logger;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.DynamicLabel;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.RnaTpmGene;
import org.nygenome.als.graphdb.value.SampleVariantSummary;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;
import scala.Tuple3;

public abstract class GraphDataConsumer implements Consumer<Path> {

  protected final String TAB_DELIM = "\t";  // tab delimited file
  protected final String COMMA_DELIM = ",";  // comma delimited file
  protected final String strNoInfo = "NA";
  protected final String strApostrophe = "`";
  protected final String HUMAN_SPECIES = "homo sapiens";
  protected Logger log = Logger.get(GraphDataConsumer.class);
  protected final Label alsLabel = new DynamicLabel("ALS-associated");


  protected final FunctionLib lib = new FunctionLib();

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
    Node goNode = lib.resolveNodeFunction.apply(new Tuple3<>(geneOntologyLabel,"GeneOntology",go.goId()));
    lib.novelLabelConsumer.accept(goNode, new DynamicLabel(go.goAspect()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyId", go.goId()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyPrinciple",
        go.goAspect()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyName", go.goName()));
    return goNode;
  };

  protected Function<String, Node> resolveSubjectNodeFunction =
      (extSubjectId) ->
     lib.resolveNodeFunction.apply( new Tuple3<>(subjectLabel,
           "SubjectId",extSubjectId));

  protected Function<String, Node> resolveHumanTissueNodeFunction = (tissueId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(tissueLabel,
          "TissueId",tissueId));

  protected Function<String, Node> resolvePathwayNodeFunction = (pathwayId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(pathwayLabel,
          "PathwayId",pathwayId));


  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(diseaseLabel,
          "DiseaseId",diseaseId));


  protected Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) -> {
    Node node = lib.resolveNodeFunction.apply(new Tuple3<>(rnaTpmLabel,
        "RnaTpmId", rnaTpmGene.id()));
    if (node != null ) {
      // persist tpm value as a String
      lib.nodePropertyValueConsumer
          .accept(node, new Tuple2<>("TPM", String.valueOf(rnaTpmGene.tpm())));
    }
    return node;
  };


  protected Function<String,Node> resolveSampleNodeFunction = (sampleId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(sampleLabel, "SampleId", sampleId));


  protected Function<String, Node> resolveGeneticEntityNodeFunction = (geneticEntityId) ->
     lib.resolveNodeFunction.apply(new Tuple3<>(geneticEntityLabel,"GeneticEntityId", geneticEntityId));

  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(proteinLabel,"UniProtKBID", uniprotId));


  protected Function<String, Node> resolveDrugBankNode = (dbId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(drugBankLabel,"DrugBankId", dbId));

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
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, transcriptNode),RelTypes.ENCODED_BY );
    });
  }
protected Function<SampleVariantSummary,Node> resolveSampleVariantNode = (svc) -> {

  Node svNode =lib.resolveNodeFunction.apply(new Tuple3<>(sampleVariantLabel,"SampleVariantId", svc.id()));
  // TODO: add test for ALS gene and add label if so
  // persist the list of variants
  lib.nodePropertyValueStringArrayConsumer.accept(svNode,new Tuple2<>("Variants", svc.variantList()));
  return svNode;
};

  protected Function<String, Node> resolveSnpNodeFunction = (snpId) ->
      lib.resolveNodeFunction.apply(new Tuple3<>(snpLabel,"SNP",snpId));


}


