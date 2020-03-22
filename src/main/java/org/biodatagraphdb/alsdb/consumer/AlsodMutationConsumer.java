package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.AlsodMutation;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.Tuple3;

/*
Java Consumer responsible for importing ALSod Muations data into
the neo4j database
 */

public class AlsodMutationConsumer extends GraphDataConsumer {

  public AlsodMutationConsumer(RunMode runMode) {
    super(runMode);
  }

  /*
  Private Consumer to map attributes from an ALSoD Mutation record to a Node in the database
  A Relationship to a HGNC gene node is also created if the ALSoD record provides a gene name
   */
  private Consumer<org.biodatagraphdb.alsdb.model.AlsodMutation> alsodMutationConsumer = (mutation) -> {
    Node mutationNode = resolveAlsodMutationNodeFunction.apply(mutation.getId());
    lib.novelLabelConsumer.accept(mutationNode, alsAssociatedLabel);
    // add properties
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("MutationName", mutation.getMutationName()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("MutationType", mutation.getMutationType()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("ChromosomeLocation", mutation.getChromosomeLocation()));
    // genetic change
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("SeqChange", mutation.getSeqChange()));
    // amino acid change
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("AA_Change", mutation.getAaChange()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("Exon/Intron", mutation.getExonOrIntron()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("HGVS_Nucleotide", mutation.getHgvsNucleotide()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("HGVS_Protein", mutation.getHgvsProtein()));
    // establish a relationship to an existing Genetic entity
    if (AlsodMutation.Companion.isValidString(mutation.getGene())) {
      Tuple3<Label, String, String> tuple3 = new Tuple3<>(geneticEntityLabel, "GeneticEntityId",
          mutation.getGene());
      lib.findExistingGraphNodeFunction.apply(tuple3).ifPresent(geneNode ->
          lib.resolveNodeRelationshipFunction
              .apply(new Tuple2<>(mutationNode, geneNode), encodedRelationType)
      );
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(AlsodMutation.Companion::parseCSVRecord)
        .forEach(alsodMutationConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(new AlsodMutationConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed alsod mustations file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new AlsodMutationConsumer(RunMode.TEST)));
  }
}
