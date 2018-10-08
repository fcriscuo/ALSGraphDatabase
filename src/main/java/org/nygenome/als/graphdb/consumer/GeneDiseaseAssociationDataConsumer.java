package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.value.UniProtMapping;
import org.nygenome.als.graphdb.service.UniProtMappingService;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.GeneDiseaseAssociation;
import scala.Tuple2;
import java.nio.file.Path;


/*
Consumer of gene disease association data from a specified file
Data mapped to data structures for entry into Neo4j database
 */

public class GeneDiseaseAssociationDataConsumer extends GraphDataConsumer {

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path);
    new TsvRecordStreamSupplier(path).get()
        .map(GeneDiseaseAssociation::parseCSVRecord)
        // ensure that this disease is associated with a protein
        .filter(gda -> UniProtMappingService.INSTANCE
            .resolveUniProtMappingFromGeneSymbol(gda.geneSymbol()).isPresent())
        .forEach(diseaseAssociationConsumer);
  }

  /*
  Private Consumer that will use the attributes in a GeneDiseaseAssociation value object
  to optionally create a new Protein Node, a new Disease Node, and a new GeneticEntity Node
  This Consumer is only invoked if the gene symbol can be mapped to a UniProt Id
   */
  private Consumer<GeneDiseaseAssociation> diseaseAssociationConsumer = (gda) -> {

    try (Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get()) {
      UniProtMapping upm = UniProtMappingService.INSTANCE
          .resolveUniProtMappingFromGeneSymbol(gda.geneSymbol())
          .get();// get from Optional is OK because of previous filter
      String uniprotId = upm.uniProtId();
      Node geneNode = resolveEnsemblGeneNodeFunction.apply(upm.ensemblGeneId());
      Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(upm.ensemblTranscriptId());
      lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("HGNCSymbol", gda.geneSymbol()));
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      Node diseaseNode = resolveDiseaseNodeFunction.apply(gda.diseaseId());
      lib.nodePropertyValueConsumer
          .accept(diseaseNode, new Tuple2<>("DiseaseName", gda.diseaseName()));
      // create relationships between these nodes
      // protein <-> transcript
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,transcriptNode),RelTypes.ENCODED_BY);
      // gene <-> transcript
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),RelTypes.TRANSCRIBES );
      // protein - disease
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,diseaseNode), RelTypes.IMPLICATED_IN);
      // gene <-> disease
      Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, diseaseNode), RelTypes.IMPLICATED_IN);
      rel.setProperty("ConfidenceLevel", gda.score());
      rel.setProperty("Reference", gda.source());
      tx.success();
    } catch (Exception e) {
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    }
  };

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new GeneDiseaseAssociationDataConsumer());
    AsyncLoggingService.logInfo("processed gene disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // main method for stand alone testing
  public static void main(String... args) {

    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new GeneDiseaseAssociationDataConsumer()));

  }


}

