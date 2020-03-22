package org.biodatagraphdb.alsdb.consumer;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.model.GeneDiseaseAssociation;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;
import java.nio.file.Path;


/*
Consumer of gene disease association data from a specified file
Data mapped to data structures for entry into Neo4j database
 */

public class
GeneDiseaseAssociationDataConsumer extends GraphDataConsumer {

  public GeneDiseaseAssociationDataConsumer(RunMode runMode) {super(runMode);}

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(GeneDiseaseAssociation.Companion::parseCSVRecord)
        // ensure that this disease is associated with a protein
        .filter(gda -> org.biodatagraphdb.alsdb.service.UniProtMappingService.INSTANCE
            .resolveUniProtMappingFromGeneSymbol(gda.getGeneSymbol()).isPresent())
        .forEach(diseaseAssociationConsumer);
  }

  /*
  Private Consumer that will use the attributes in a GeneDiseaseAssociation value object
  to optionally create a new Protein Node, a new Disease Node, and a new GeneticEntity Node
  This Consumer is only invoked if the gene symbol can be mapped to a UniProt Id
   */
  private Consumer<org.biodatagraphdb.alsdb.model.GeneDiseaseAssociation> diseaseAssociationConsumer = (gda) -> {
      org.biodatagraphdb.alsdb.model.UniProtMapping upm = org.biodatagraphdb.alsdb.service.UniProtMappingService.INSTANCE
          .resolveUniProtMappingFromGeneSymbol(gda.getGeneSymbol())
          .get();// get from Optional is OK because of previous filter
      String uniprotId = upm.getUniProtId();
      Node geneNode = resolveEnsemblGeneNodeFunction.apply(upm.getEnsemblGeneId());
      Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(upm.getEnsemblTranscriptId());
      lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("HGNCSymbol", gda.getGeneSymbol()));
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      Node diseaseNode = resolveDiseaseNodeFunction.apply(gda.getDiseaseId());
      lib.nodePropertyValueConsumer
          .accept(diseaseNode, new Tuple2<>("DiseaseName", gda.getDiseaseName()));
      // create relationships between these nodes
      // protein <-> transcript
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,transcriptNode),encodedRelationType);
      // gene <-> transcript
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),transcribesRelationType );
      // protein - disease
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,diseaseNode), implicatedInRelationType);
      // gene <-> disease
      Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, diseaseNode),
         implicatedInRelationType);
      lib.relationshipPropertyValueConsumer.accept(rel,new Tuple2<>("ConfidenceLevel", String.valueOf(gda.getScore())));
      lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Reference", gda.getSource()));

  };

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new GeneDiseaseAssociationDataConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed gene disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // main method for stand alone testing
  public static void main(String... args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer().accept(path, new GeneDiseaseAssociationDataConsumer(RunMode.TEST)));

  }


}

