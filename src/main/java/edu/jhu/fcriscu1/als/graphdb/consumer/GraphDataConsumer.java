package edu.jhu.fcriscu1.als.graphdb.consumer;


import com.twitter.util.Function3;
import java.nio.file.Path;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.DynamicLabel;
import edu.jhu.fcriscu1.als.graphdb.util.DynamicRelationshipTypes;
import edu.jhu.fcriscu1.als.graphdb.value.GeneOntology;
import edu.jhu.fcriscu1.als.graphdb.value.RnaTpmGene;
import edu.jhu.fcriscu1.als.graphdb.value.SampleVariantSummary;
import edu.jhu.fcriscu1.als.graphdb.value.UniProtValue;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import edu.jhu.fcriscu1.als.graphdb.lib.FunctionLib;
import edu.jhu.fcriscu1.als.graphdb.util.StringUtils;
import scala.Tuple2;
import scala.Tuple3;

public abstract class GraphDataConsumer implements Consumer<Path> {



  protected final String HUMAN_SPECIES = "homo sapiens";

  // defined Labels
  protected  FunctionLib lib;
  protected final Label alsAssociatedLabel = new DynamicLabel("ALS-associated");
  protected final Label subjectLabel  = new DynamicLabel("Subject");
  protected final Label sampleLabel  = new DynamicLabel("Sample");
  protected final Label tissueLabel = new DynamicLabel("Tissue");
  protected final Label pathwayLabel = new DynamicLabel("Pathway");
  protected final Label diseaseLabel = new DynamicLabel("Disease");
  protected final Label rnaTpmLabel = new DynamicLabel("RnaTpm");
  protected final Label geneticEntityLabel = new DynamicLabel("GeneticEntity");
  protected final Label proteinLabel  = new DynamicLabel("Protein");
  protected final Label drugBankLabel = new DynamicLabel("DrugBank");
  protected final Label transcriptLabel = new DynamicLabel("Transcript");
  protected final Label geneLabel = new DynamicLabel("Gene");
  protected final Label geneOntologyLabel = new DynamicLabel("GeneOntology");
  protected final Label sampleVariantLabel = new DynamicLabel("SampleVariant");
  protected final Label snpLabel = new DynamicLabel("SNP");
  protected final Label neurobankLabel = new DynamicLabel("Neurobank");
  protected final Label proactLabel = new DynamicLabel("PRO-ACT");
  protected final Label neurobankCategoryLabel = new DynamicLabel("NeurobankCategory");
  protected final Label subjectPropertyLabel = new DynamicLabel("SubjectProperty");
  protected final Label alsStudyTimepointLabel = new DynamicLabel("AlsStudyTimepoint");
  protected final Label alsStudyTimepointEventLabel = new DynamicLabel("AlsStudyTimepointEvent");
  protected final Label subjectEventPropertyLabel = new DynamicLabel("SubjectEventProperty");
  protected final Label subjectEventPropertyValueLabel = new DynamicLabel("SubjectEventPropertyValue");
  protected final Label xrefLabel = new DynamicLabel("Xref");
  protected final Label hgncLabel = new DynamicLabel("HGNC");
  protected final Label ensemblLabel = new DynamicLabel("ensembl");
  protected final Label pubMedLabel = new DynamicLabel("PubMed");
  protected final Label ccdsLabel = new DynamicLabel("CCDS");
  protected final Label entrezLabel = new DynamicLabel("Entrez");
  protected final Label omimLabel = new DynamicLabel("Omim");
  protected final Label refSeqLabel = new DynamicLabel("RefSeq");
  protected final Label alsodMutationLabel = new DynamicLabel("ALSoDMutation");
  protected final Label proteinCodingLabel = new DynamicLabel("ProteinCodingGene");
  protected final Label nonCodingRNALabel = new DynamicLabel("Non-codingRNA");

  protected final Label proactAdverseEventLabel = new DynamicLabel("AdverseEvent");
  protected final Label systemOrganClassLabel = new DynamicLabel("SystemOrganClass");
  protected final Label highLevelGroupTermLabel = new DynamicLabel("HighLevelGroupTerm");
  protected final Label highLevelTermLabel = new DynamicLabel("HighLevelTerm");
  protected final Label preferredTermLabel = new DynamicLabel("PreferredTerm");
  protected final Label lowestLevelTermLabel = new DynamicLabel("LowestLevelTerm");
 // defined relationship types
  protected final RelationshipType transcribesRelationType = new DynamicRelationshipTypes("TRANSCRIBES");
  protected final RelationshipType xrefRelationType = new DynamicRelationshipTypes("REFERENCES");
  protected final RelationshipType encodedRelationType = new DynamicRelationshipTypes("ENCODED_BY");
  protected final RelationshipType noEventRealtionshipType = new DynamicRelationshipTypes("NO_EVENT");
  protected final RelationshipType pathwayRelationshipType = new DynamicRelationshipTypes("IN_PATHWAY");
  protected final RelationshipType biomarkerRelationType = new DynamicRelationshipTypes("IS_BIOMARKER");
  protected final RelationshipType therapeuticRelationType = new DynamicRelationshipTypes("IS_THERAPEUTIC");
  protected final RelationshipType geneticVariationRelationType = new DynamicRelationshipTypes("HAS_GENETIC_VARIATION");
  protected final RelationshipType alsAssoctiatedRelationType = new DynamicRelationshipTypes("IS_ALS_ASSOCIATED");
  protected final RelationshipType ppiAssociationRelationType = new DynamicRelationshipTypes("ASSOCIATES");
  protected final RelationshipType ppiColocalizationRelationType = new DynamicRelationshipTypes("CO-LOCALIZES");
  protected final RelationshipType ppiGeneticInteractionRelationType = new DynamicRelationshipTypes("HAS_GENETIC_INTERACTION");
  protected final RelationshipType ppiPredictedInteractionRelationType = new DynamicRelationshipTypes("HAS_PREDICTED_INTERACTION");
  protected final RelationshipType tissueEnhancedRelationType = new DynamicRelationshipTypes("IS_ENHANCED_IN");
  protected final RelationshipType drugTargetRelationType = new DynamicRelationshipTypes("IS_DRUG_TARGET");
  protected final RelationshipType drugEnzymeRelationType = new DynamicRelationshipTypes("IS_DRUG_ENZYME");
  protected final RelationshipType drugTransporterRelationType = new DynamicRelationshipTypes("IS_DRUG_TRANSPORTER");
  protected final RelationshipType drugCarrierRelationType = new DynamicRelationshipTypes("IS_DRUG_CARRIER");
  protected final RelationshipType partOfRelationType = new DynamicRelationshipTypes("IS_PART_OF");
  protected final RelationshipType degRealtedToRelationType = new DynamicRelationshipTypes("IS_DEG_RELATED_TO");
  protected final RelationshipType seqSimRelationType = new DynamicRelationshipTypes("HAS SEQ_SIMILARITY_TO");
  protected final RelationshipType goClassificatioRelationType = new DynamicRelationshipTypes("HAS_GO_CLASSIFICATION");
  protected final RelationshipType transcriptRelationType = new DynamicRelationshipTypes("TRANSCRIBES");
  protected final RelationshipType implicatedInRelationType = new DynamicRelationshipTypes("IS_IMPLICATED_IN");
  protected final RelationshipType sampleRelationType = new DynamicRelationshipTypes("HAS_SAMPLE");
  protected final RelationshipType sampledFromRelationType = new DynamicRelationshipTypes("SAMPLED_FROM");
  protected final RelationshipType mapsToRelationType = new DynamicRelationshipTypes("MAPS_TO");
  protected final RelationshipType expressionLevelRelationType = new DynamicRelationshipTypes("HAS_EXPRESS_LEVEL");
  protected final RelationshipType expressedProteinRelationType = new DynamicRelationshipTypes("EXPRESSES_PROTEIN");
  protected final RelationshipType associatedProteinRelationType = new DynamicRelationshipTypes("ASSOCIATED_PROTEIN");
  protected final RelationshipType geneticEntityRelationType = new DynamicRelationshipTypes("ASSOCIATED_GENETIC_ENTITY");
  protected final RelationshipType variantRelationType = new DynamicRelationshipTypes("ASSOCIATED_VARIANT");
  protected final RelationshipType childRelationType = new DynamicRelationshipTypes("IS_CHILD_OF");
  protected final RelationshipType categorizesRelType = new DynamicRelationshipTypes("CATEGORIZES");
  protected final RelationshipType propertyRelationType = new DynamicRelationshipTypes("HAS_PROPERTY");
  protected final RelationshipType subjectEventRelationType = new DynamicRelationshipTypes("HAS_SUBJECT_EVENT");
  protected final RelationshipType timepointtRelationType = new DynamicRelationshipTypes("OCCURRED_AT");
  protected final RelationshipType goBioProcessRelType = new DynamicRelationshipTypes("HAS_GO_BIO_PROCESS");
  protected final RelationshipType goCellComponentRelType = new DynamicRelationshipTypes("HAS_GO_CELLULAR_COMPONENT");
  protected final RelationshipType goMolFunctionRelType = new DynamicRelationshipTypes("HAS_GO_MOLECULAR_FUNCTION");
  protected final RelationshipType pubMedXrefRelType = new DynamicRelationshipTypes("HAS_PUBMED_XREF");

    protected final RelationshipType eventValueSubjectRelType = new DynamicRelationshipTypes(
            "HAS_PROPERTY");
    protected final RelationshipType eventValueEventRelType = new DynamicRelationshipTypes(
            "HAS_EVENT_OCCURRANCE"
    );
    protected final RelationshipType eventTimepointRelType = new DynamicRelationshipTypes(
            "OCCURRED_AT"
    );


  private GraphDatabaseServiceSupplier.RunMode runMode;
  protected  GraphDataConsumer(@Nonnull GraphDatabaseServiceSupplier.RunMode runMode) {
    this.lib = new FunctionLib(runMode);
    this.runMode = runMode;
  }

  public Supplier<GraphDatabaseServiceSupplier.RunMode> consumerRunModeSupplier = () -> this.runMode;


  protected Function<String,Node> resolveProactAdverseEventNode = (id) ->{
   Node aeNode = lib.resolveGraphNodeFunction.apply(new Tuple3<>(proactAdverseEventLabel,
     "AdverseEventId",id  ));
   lib.novelLabelConsumer.accept(aeNode, alsAssociatedLabel);
   lib.novelLabelConsumer.accept(aeNode, proactLabel);
   return aeNode;
 };


  /*
  Consumer that ensures that an ALS-associated Node is properly annotated
   */
  protected Consumer<Node> annotateNeurobankNodeConsumer = (node)-> {
    lib.novelLabelConsumer.accept(node, neurobankLabel);
    lib.novelLabelConsumer.accept(node, alsAssociatedLabel);
  };

  protected Function<GeneOntology,Node> resolveGeneOntologyNodeFunction = (go)-> {
    Node goNode = lib.resolveGraphNodeFunction
        .apply(new Tuple3<>(geneOntologyLabel,"GeneOntology",go.goTermAccession()));
    lib.novelLabelConsumer.accept(goNode,xrefLabel);
    if (GeneOntology.isValidString(go.goDomain())) {
      lib.novelLabelConsumer.accept(goNode, new DynamicLabel(go.goDomain()));
    }
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyTermAccession", go.goTermAccession()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyDomain",
        go.goDomain()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyTermName", go.goName()));
    lib.nodePropertyValueConsumer.accept(goNode, new Tuple2<>("GeneOntologyTermDefinition", go.goDefinition()));
    return goNode;
  };

  protected Function<String,Node> resolveAlsodMutationNodeFunction  = (id)->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(alsodMutationLabel, "ALSoDMutationCode", id));

  protected Function<String,Node> resolveSubjectEventPropertyValueNodeFunction = (id) ->
      lib.resolveGraphNodeFunction
          .apply(new Tuple3<>(subjectEventPropertyValueLabel,"EventPropertyValue",id));

  protected Function<String,Node> resolveSubjectEventPropertyNodeFunction = (id) ->
    lib.resolveGraphNodeFunction.apply(new Tuple3<>(subjectEventPropertyLabel,"EventProperty",id));

  /*
  Function to find or create a new Xref Node
  Requires a secondary Label to identify the xref source
   */
  protected BiFunction<Label,String,Node> resolveXrefNode = (label, id)-> {
    Node node = lib.resolveGraphNodeFunction.apply(new Tuple3<>(xrefLabel, "XrefId", id));
    lib.novelLabelConsumer.accept(node, label);
    return node;
  };

  /*
A Protected Function that will find/create an Xref Node using a specified identifier and the
standard xref Label. The supplied Label will be added if the xref Node does not already have that
Label. A xref Relationship, between the supplied Node and the resolved xref node, will
be created and returned
 */
  protected Function3<Node,Label,String, Relationship> registerXrefRelationshipFunction
      = new Function3<Node, Label, String, Relationship>() {
    @Override
    public Relationship apply(Node sourceNode, Label secondaryLabel, String xrefId) {
      Node xrefNode = resolveXrefNode.apply(xrefLabel,xrefId);
      lib.novelLabelConsumer.accept(xrefNode,secondaryLabel);
      return lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(sourceNode,xrefNode),xrefRelationType);
    }
  };

  protected Function<Tuple2<String,String>, Node>  resolveEventTimepointNodeFunction = (tuple) ->{
    Node node =  lib.resolveGraphNodeFunction.apply(new Tuple3<>(alsStudyTimepointLabel,"TimepointId",tuple._1()));
    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("TimepintName",tuple._2()));
    return node;
  };
  /*
  resolveSubjectEventNodeFunction.apply(property.subjectEventTuple());
   */
  protected Function<Tuple2<String,String>,Node> resolveSubjectEventNodeFunction = (tuple) -> {
    Node node = lib.resolveGraphNodeFunction.apply(new Tuple3<>(alsStudyTimepointEventLabel,
        "EventCategory",tuple._1()));
    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("FormName", tuple._2()));
    return node;
  };



  protected Function<String,Node> resolveSubjectPropertyNode = (id) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(subjectPropertyLabel, "Id",id));

  protected Function<String,Node> resolveCategoryNode = (category) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(neurobankCategoryLabel, "Category",category));

  protected Function<Tuple2<String,String>, Node> resolveSubjectNodeFunction =
      (subjectTuple) -> {
        Node node = lib.resolveGraphNodeFunction.apply(new Tuple3<>(subjectLabel,
            "SubjectGuid", subjectTuple._2()));
        lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("SubjectId", subjectTuple._1()));
        return node;
      };



  protected Function<String, Node> resolveHumanTissueNodeFunction = (tissueId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(tissueLabel,
          "TissueId",tissueId));

  protected Function<String, Node> resolvePathwayNodeFunction = (pathwayId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(pathwayLabel,
          "PathwayId",pathwayId));


  protected Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(diseaseLabel,
          "DiseaseId",diseaseId));

  protected Function<RnaTpmGene, Node> resolveRnaTpmGeneNode = (rnaTpmGene) -> {
    Node node = lib.resolveGraphNodeFunction.apply(new Tuple3<>(rnaTpmLabel,
        "RnaTpmId", rnaTpmGene.id()));
    if (node != null ) {
      // persist tpm value as a String
      lib.nodePropertyValueConsumer
          .accept(node, new Tuple2<>("TPM", String.valueOf(rnaTpmGene.tpm())));
    }
    return node;
  };

  protected Function<String,Node> resolveSampleNodeFunction = (sampleId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(sampleLabel, "SampleId", sampleId));


  protected Function<String, Node> resolveGeneticEntityNodeFunction = (geneticEntityId) ->
     lib.resolveGraphNodeFunction
         .apply(new Tuple3<>(geneticEntityLabel,"GeneticEntityId", geneticEntityId));
  /*
  Protected Function that resolves a Protein Node for a specified UniProt id
  by either finding an existing Node or by creating a new one
   */
  protected Function<String, Node> resolveProteinNodeFunction = (uniprotId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(proteinLabel,"UniProtKBID", uniprotId));


  protected Function<String, Node> resolveDrugBankNode = (dbId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(drugBankLabel,"DrugBankId", dbId));

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
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, transcriptNode),
          encodedRelationType );
    });
  }
protected Function<SampleVariantSummary,Node> resolveSampleVariantNode = (svc) -> {

  Node svNode =lib.resolveGraphNodeFunction
      .apply(new Tuple3<>(sampleVariantLabel,"SampleVariantId", svc.id()));
  // persist the list of variants
  lib.nodePropertyValueStringArrayConsumer.accept(svNode,new Tuple2<>("Variants", svc.variantList()));
  return svNode;
};

  protected Function<String, Node> resolveSnpNodeFunction = (snpId) ->
      lib.resolveGraphNodeFunction.apply(new Tuple3<>(snpLabel,"SNP",snpId));

}


