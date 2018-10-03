package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.app.EmbeddedGraphApp.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.UniProtValue;
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

  private void createProteinGeneOntologyRealtionship(String uniprotId, GeneOntology go) {
    // establish relationship to the protein node
    Tuple2<String, String> relKey = new Tuple2<>(uniprotId, go.goId());
    if (!proteinGeneOntologyRelMap.containsKey(relKey)) {
      Node goNode = resolveGeneOntologyNodeFunction.apply(go);
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      lib.createUniDirectionalRelationship(proteinNode,goNode,
          new Tuple2<>(uniprotId, go.goId()),proteinGeneOntologyRelMap,
          RelTypes.GO_CLASSIFICATION);

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

  private Consumer<UniProtValue> uniProtValueConsumer = (upv -> {
    // create the protein node for this uniprot entry
    uniProtValueToProteinNodeConsumer.accept(upv);
    // add Gene Ontology associations
    geneOntologyListConsumer.accept(upv);
    // add ensembl trancripts
    createEnsemblTranscriptNodes(upv);
    // add drugs associated with this protein
    drugBankIdConsumer.accept(upv);

  });

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_HUMAN_FILE")
        .ifPresent(new UniProtValueConsumer());
  }


}
