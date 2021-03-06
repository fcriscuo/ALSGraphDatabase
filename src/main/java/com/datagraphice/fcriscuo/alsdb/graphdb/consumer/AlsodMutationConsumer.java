package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.AlsodMutation;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.Tuple3;

/*
Java Consumer responsible for importing ALSod Muations data into
the neo4j database
 */

public class AlsodMutationConsumer extends GraphDataConsumer {

  public AlsodMutationConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  /*
  Private Consumer to map attributes from an ALSoD Mutation record to a Node in the database
  A Relationship to a HGNC gene node is also created if the ALSoD record provides a gene name
   */
  private Consumer<AlsodMutation> alsodMutationConsumer = (mutation) -> {
    Node mutationNode = resolveAlsodMutationNodeFunction.apply(mutation.id());
    lib.getNovelLabelConsumer().accept(mutationNode, alsAssociatedLabel);
    // add properties
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("MutationName", mutation.mutationName()));
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("MutationType", mutation.mutationType()));
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("ChromosomeLocation", mutation.chromosomeLocation()));
    // genetic change
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("SeqChange", mutation.seqChange()));
    // amino acid change
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("AA_Change", mutation.aaChange()));
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("Exon/Intron", mutation.exonOrIntron()));
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("HGVS_Nucleotide", mutation.hgvsNucleotide()));
    lib.getNodePropertyValueConsumer()
        .accept(mutationNode, new Tuple2<>("HGVS_Protein", mutation.hgvsProtein()));
    // establish a relationship to an existing Genetic entity
    if (AlsodMutation.isValidString(mutation.gene())) {
      Tuple3<Label, String, String> tuple3 = new Tuple3<>(geneticEntityLabel, "GeneticEntityId",
          mutation.gene());
      lib.getFindExistingGraphNodeFunction().apply(tuple3).ifPresent(geneNode ->
          lib.getResolveNodeRelationshipFunction()
              .apply(new Tuple2<>(mutationNode, geneNode), encodedRelationType)
      );
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier(path).get()
        .map(AlsodMutation::parseCSVRecord)
        .forEach(alsodMutationConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(new AlsodMutationConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed alsod mustations file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new AlsodMutationConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
