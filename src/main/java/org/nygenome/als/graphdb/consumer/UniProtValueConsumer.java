package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.DynamicRelationshipTypes;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.UniProtValue;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.value.GeneOntology;
import scala.Tuple2;


public class UniProtValueConsumer extends GraphDataConsumer {

  public UniProtValueConsumer() {}
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(UniProtValue::parseCSVRecord)
        //.filter(upv->UniProtValue.isValidString(upv.uniprotId()))
        .forEach(uniProtValueConsumer);
  }

  private void createProteinGeneOntologyRealtionship(String uniprotId, GeneOntology go, RelationshipType relType) {
    // establish relationship to the protein node
      Node goNode = resolveGeneOntologyNodeFunction.apply(go);
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);

      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, goNode),
          relType);
      AsyncLoggingService.logInfo("Created relationship between protein " + uniprotId
          + " and GO id: " + go.goTermAccession());
  }

  private Consumer<UniProtValue> geneOntologyListConsumer = (upv) -> {
    // complete the association with gene ontology links
    // biological process
    StringUtils.convertToJavaString(upv.goBioProcessList())
        .stream()
        .map(goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (bio process)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go,
           goBioProcessRelType));
    // cellular  component
    StringUtils.convertToJavaString(upv.goCellComponentList())
        .stream()
        .map(goEntry -> GeneOntology
            .parseGeneOntologyEntry("Gene Ontology (cellular component)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go,
            goCellComponentRelType));
    // molecular function
    StringUtils.convertToJavaString(upv.goMolFuncList())
        .stream()
        .map(
            goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (mol function)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go,
            goMolFunctionRelType));

  };

  private Consumer<UniProtValue> uniProtValueToProteinNodeConsumer = (upv) -> {
      Node node = resolveProteinNodeFunction.apply(upv.uniprotId());
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("UniProtName", upv.uniprotName()));
      lib.nodePropertyValueListConsumer.accept(node, new Tuple2<>("ProteinName", upv.proteinNameList()));
      lib.nodePropertyValueListConsumer.accept(node, new Tuple2<>("GeneSymbol", upv.geneNameList()));
      lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("Mass", upv.mass()));
    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("Length", upv.length()));
      AsyncLoggingService.logInfo(">>>Created Protein node for " +upv.uniprotId());

  };

  // create drugbank nodes associated with this protein
  // to protein - drug relationships are determined by other
  // source files
  private Consumer<UniProtValue> drugBankIdConsumer = (upv) ->
      StringUtils.convertToJavaString(upv.drugBankIdList())
      .forEach(dbId ->resolveDrugBankNode.apply(dbId));

  private Consumer<UniProtValue> pubMedXrefConsumer = (upv) -> {
    // PubMed Xrefs
    Node proteinNode = resolveProteinNodeFunction.apply(upv.uniprotId());
    StringUtils.convertToJavaString(upv.pubMedIdList())
        .stream()
        .map(pubMedId ->resolveXrefNode.apply(pubMedLabel,pubMedId))
        .forEach(xrefNode -> lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, xrefNode),
            pubMedXrefRelType));
  };

  private Consumer<UniProtValue> uniProtValueConsumer = (upv -> {
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
  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_HUMAN_FILE")
        .ifPresent(new UniProtValueConsumer());
    AsyncLoggingService.logInfo("read uniprot data: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds.");
  }

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_UNIPROT_HUMAN_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new UniProtValueConsumer()));
  }


}
