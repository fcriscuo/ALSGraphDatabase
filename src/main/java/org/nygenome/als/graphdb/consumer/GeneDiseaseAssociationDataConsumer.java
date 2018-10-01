package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
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

    try ( Transaction tx = EmbeddedGraph.INSTANCE.transactionSupplier.get()) {
      String uniprotId = UniProtMappingService.INSTANCE
          .resolveUniProtMappingFromGeneSymbol(gda.geneSymbol())
          .get().uniProtId();   // get from Optional is OK because of previous filter

      Node geneNode = resolveGeneNodeFunction.apply(gda.geneSymbol());
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      String diseaseId = gda.diseaseId();
      Node diseaseNode = resolveDiseaseNodeFunction.apply(diseaseId);
      nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseName", gda.diseaseName()));
      // create bi-directional relationships between these nodes
      // protein - gene
      lib.createBiDirectionalRelationship(proteinNode, geneNode,
          new Tuple2<>(uniprotId, gda.geneSymbol()),
          proteinGeneticEntityMap, RelTypes.ENCODED_BY, RelTypes.EXPRESSED_PROTEIN
      );
      // protein - disease
      lib.createBiDirectionalRelationship(proteinNode, diseaseNode,
          new Tuple2<>(uniprotId, gda.diseaseId()),
          proteinDiseaseRelMap, RelTypes.IMPLICATED_IN, RelTypes.ASSOCIATED_PROTEIN);
      // gene -disease
      Tuple2<String, String> geneDiseaseTuple = new Tuple2<>(gda.geneSymbol(), diseaseId);
      lib.createBiDirectionalRelationship(geneNode, diseaseNode, geneDiseaseTuple,
          geneticEntityDiseaseMap, RelTypes.IMPLICATED_IN, RelTypes.ASSOCIATED_GENETIC_ENTITY);
      geneticEntityDiseaseMap.get(geneDiseaseTuple).setProperty("ConfidenceLevel", gda.score());
      geneticEntityDiseaseMap.get(geneDiseaseTuple).setProperty("Reference", gda.source());
      tx.success();
    } catch (Exception e) {
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    }
  };
  // main method for stand alone testing
  public static void main(String... args) {

    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new  TestGraphDataConsumer().accept(path, new GeneDiseaseAssociationDataConsumer()));

  }



}

