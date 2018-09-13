package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.nygenome.als.graphdb.service.DrugBankService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.GeneOntology;
import org.nygenome.als.graphdb.value.UniProtValue;


public class UniProtValueConsumer extends GraphDataConsumer {

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(UniProtValue::parseCSVRecord)
        //.filter(upv->UniProtValue.isValidString(upv.uniprotId()))
        .forEach(uniProtValueConsumer);
  }

  private Consumer<UniProtValue> geneOntologyListConsumer = (upv) -> {
    // complete the association with gene ontology links
    // biological process
    StringUtils.convertToJavaString(upv.goBioProcessList())
        .stream()
        .map(goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (bio process)", goEntry))
        .forEach(go -> createGeneOntologyNode(upv.uniprotId(), go));
    // cellular  component
    StringUtils.convertToJavaString(upv.goCellComponentList())
        .stream()
        .map(goEntry -> GeneOntology
            .parseGeneOntologyEntry("Gene Ontology (cellular component)", goEntry))
        .forEach(go -> createGeneOntologyNode(upv.uniprotId(), go));
    // molecular function
    StringUtils.convertToJavaString(upv.goMolFuncList())
        .stream()
        .map(
            goEntry -> GeneOntology.parseGeneOntologyEntry("Gene Ontology (mol function)", goEntry))
        .forEach(go -> createGeneOntologyNode(upv.uniprotId(), go));
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
    createProteinNode(upv);
    // add Gene Ontology associations
    geneOntologyListConsumer.accept(upv);
    // add ensembl trancripts
    createEnsemblTranscriptNodes(upv);


  });


}
