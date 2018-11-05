package edu.jhu.fcriscu1.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import edu.jhu.fcriscu1.als.graphdb.integration.TestGraphDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.GeneDiseaseAssociation;
import edu.jhu.fcriscu1.als.graphdb.value.UniProtMapping;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import edu.jhu.fcriscu1.als.graphdb.service.UniProtMappingService;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import scala.Tuple2;
import java.nio.file.Path;


/*
Consumer of gene disease association data from a specified file
Data mapped to data structures for entry into Neo4j database
 */

public class GeneDiseaseAssociationDataConsumer extends GraphDataConsumer {

  public GeneDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

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
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,transcriptNode),encodedRelationType);
      // gene <-> transcript
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),transcribesRelationType );
      // protein - disease
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,diseaseNode), implicatedInRelationType);
      // gene <-> disease
      Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, diseaseNode),
         implicatedInRelationType);
      lib.relationshipPropertyValueConsumer.accept(rel,new Tuple2<>("ConfidenceLevel", String.valueOf(gda.score())));
      lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Reference", gda.source()));

  };

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new GeneDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed gene disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // main method for stand alone testing
  public static void main(String... args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new GeneDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));

  }


}

