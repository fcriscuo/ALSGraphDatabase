package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.GeneOntology;
import org.biodatagraphdb.alsdb.model.UniProtValue;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

public class UniProtValueConsumer extends GraphDataConsumer {

  public UniProtValueConsumer(RunMode runMode) {super(runMode);}
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(UniProtValue.Companion::parseCSVRecord)
        //.filter(upv->UniProtValue.isValidString(upv.uniprotId()))
        .forEach(uniProtValueConsumer);
    lib.shutDown();
  }

  private void createProteinGeneOntologyRealtionship(String uniprotId, org.biodatagraphdb.alsdb.model.GeneOntology go, RelationshipType relType) {
    // establish relationship to the protein node
      Node goNode = resolveGeneOntologyNodeFunction.apply(go);
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);

      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, goNode),
          relType);
      AsyncLoggingService.logInfo("Created relationship between protein " + uniprotId
          + " and GO id: " + go.getGoTermAccession());
  }

  private Consumer<org.biodatagraphdb.alsdb.model.UniProtValue> geneOntologyListConsumer = (upv) -> {
    // complete the association with gene ontology links
    // biological process
    upv.getGoBioProcessList()
        .stream()
        .map(goEntry -> GeneOntology.Companion.parseGeneOntologyEntry("Gene Ontology (bio process)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.getUniprotId(), go,
           goBioProcessRelType));
    // cellular  component
    upv.getGoCellComponentList()
        .stream()
        .map(goEntry -> GeneOntology.Companion
            .parseGeneOntologyEntry("Gene Ontology (cellular component)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.getUniprotId(), go,
            goCellComponentRelType));
    // molecular function
    upv.getGoMolFuncList()
        .stream()
        .map(
            goEntry -> GeneOntology.Companion.parseGeneOntologyEntry("Gene Ontology (mol function)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.getUniprotId(), go,
            goMolFunctionRelType));

  };

  private Consumer<UniProtValue> uniProtValueToProteinNodeConsumer = (upv) -> {
      Node node = resolveProteinNodeFunction.apply(upv.getUniprotId());
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtName", upv.getUniprotName()));
      lib.nodePropertyValueListConsumer.accept(node, new Tuple2<>("ProteinName", upv.getProteinNameList()));
      lib.nodePropertyValueListConsumer.accept(node, new Tuple2<>("GeneSymbol", upv.getGeneNameList()));
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("Mass", upv.getMass()));
    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("Length", upv.getLength()));
      AsyncLoggingService.logInfo(">>>Created Protein node for " +upv.getUniprotId());

  };

  // create drugbank nodes associated with this protein
  // to protein - drug relationships are determined by other
  // source files
  private Consumer<UniProtValue> drugBankIdConsumer = upv ->
      upv.getDrugBankIdList()
      .forEach(dbId ->resolveDrugBankNode.apply(dbId));

  private Consumer<UniProtValue> pubMedXrefConsumer = upv -> {
    // PubMed Xrefs
    Node proteinNode = resolveProteinNodeFunction.apply(upv.getUniprotId());
    upv.getPubMedIdList()
        .stream()
        .map(pubMedId ->resolveXrefNode.apply(pubMedLabel,pubMedId))
        .forEach(xrefNode -> lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, xrefNode),
            pubMedXrefRelType));
  };

  private Consumer<org.biodatagraphdb.alsdb.model.UniProtValue> uniProtValueConsumer = (upv -> {
    // create the protein node for this uniprot entry
    uniProtValueToProteinNodeConsumer.accept(upv);
    // add Gene Ontology associations
    geneOntologyListConsumer.accept(upv);
    // add ensembl trancripts
    //createEnsemblTranscriptNodes(upv);
    // add drugs associated with this protein
    drugBankIdConsumer.accept(upv);
    // pubmed
    pubMedXrefConsumer.accept(upv);

  });
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_HUMAN_FILE")
        .ifPresent(new UniProtValueConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("read uniprot data: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds.");
  }

  public static void main(String[] args) {

    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_UNIPROT_HUMAN_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new UniProtValueConsumer(RunMode.TEST)));
  }


}
