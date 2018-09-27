package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.service.DrugBankService;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.UniProtValue;
import scala.Tuple2;


public class UniProtValueConsumer extends GraphDataConsumer {

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(UniProtValue::parseCSVRecord)
        //.filter(upv->UniProtValue.isValidString(upv.uniprotId()))
        .forEach(uniProtValueConsumer);
  }

  private void createProteinGeneOntologyRealtionship(String uniprotId, GeneOntology go) {
    // establish relationship to the protein node
    Tuple2<String, String> relKey = new Tuple2<>(uniprotId, go.goId());
    if (!proteinGeneOntologyRelMap.containsKey(relKey)) {
      Node goNode = resolveGeneOntologyNodeFunction.apply(go);
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      proteinGeneOntologyRelMap.put(relKey,
          proteinNode.createRelationshipTo(goNode, RelTypes.GO_CLASSIFICATION)
      );
      AsyncLoggingService.logInfo("Created relationship between protein " + uniprotId
          + " and GO id: " + go.goId());
    }
  }

  private Consumer<UniProtValue> geneOntologyListConsumer = (upv) -> {
    // complete the association with gene ontology links
    // biological process
    StringUtils.convertToJavaString(upv.goBioProcessList())
        .stream()
        .map(goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (bio process)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go));
    // cellular  component
    StringUtils.convertToJavaString(upv.goCellComponentList())
        .stream()
        .map(goEntry -> GeneOntology
            .parseGeneOntologyEntry("Gene Ontology (cellular component)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go));
    // molecular function
    StringUtils.convertToJavaString(upv.goMolFuncList())
        .stream()
        .map(
            goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (mol function)", goEntry))
        .forEach(go -> createProteinGeneOntologyRealtionship(upv.uniprotId(), go));
  };
  /*
  Private Consumer to add drug bank nodes
   */
  private Consumer<UniProtValue> drugBankIdConsumer = (upv) -> {
    StringUtils.convertToJavaString(upv.drugBankIdList()).stream()
        .map(DrugBankService.INSTANCE::getDrugBankValueById)
        .forEach(dbOpt ->dbOpt.ifPresent(db ->createDrugBankNode(upv.uniprotId(), db)));
  };


  private Consumer<UniProtValue> uniProtValueConsumer = (upv -> {
    // create the protein node for this uniprot entry
    uniProtValueToProteinNodeConsumer.accept(upv);
    // add Gene Ontology associations
    geneOntologyListConsumer.accept(upv);
    // add ensembl trancripts
    createEnsemblTranscriptNodes(upv);

  });

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_UNIPROT_HUMAN_FILE")
        .ifPresent(new UniProtValueConsumer());
  }


}
