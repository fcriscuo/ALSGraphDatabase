package org.nygenome.als.graphdb.consumer;


import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.service.UniProtMappingService;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.GeneDiseaseAssociation;
import scala.Tuple2;
import java.nio.file.Path;
import java.util.Map;

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


  private Function<String, Node> resolveDiseaseNodeFunction = (diseaseId) -> {
    if (!diseaseMap.containsKey(diseaseId)) {
      AsyncLoggingService.logInfo("createDiseasekNode invoked for Disease id  " +
          diseaseId);
      diseaseMap.put(diseaseId, EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Disease));
    }
    return diseaseMap.get(diseaseId);
  };

  private Consumer<GeneDiseaseAssociation> diseaseAssociationConsumer = (gda) -> {

    String uniprotId = UniProtMappingService.INSTANCE
        .resolveUniProtMappingFromGeneSymbol(gda.geneSymbol())
        .get().uniProtId();   // get from Optional is OK because of previous filter
    Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
    String diseaseId = gda.diseaseId();
    Node diseaseNode = resolveDiseaseNodeFunction.apply(diseaseId);
    nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseName", gda.diseaseName()));
    nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseId", diseaseId));

    // register protein-disease relationship if new

    Tuple2<String, String> protDisTuple = new Tuple2<>(uniprotId, diseaseId);
    if (!proteinDiseaseRelMap.containsKey(protDisTuple)) {
      proteinDiseaseRelMap.put(protDisTuple,
          proteinNode.createRelationshipTo(diseaseNode, RelTypes.IMPLICATED_IN)
      );
    }

    proteinDiseaseRelMap.get(protDisTuple).setProperty("Confidence_level",
        gda.score());
    proteinDiseaseRelMap.get(protDisTuple).setProperty("Reference",
        gda.source());

  };

  // main method for stand alone testing
  public static void main(String... args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_UNIPROT_ID_ASSOC_DISGENET_FILE")
        .ifPresent(new GeneUniprotIdAssociationDataConsumer());
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new GeneUniprotIdAssociationDataConsumer());


  }

}

