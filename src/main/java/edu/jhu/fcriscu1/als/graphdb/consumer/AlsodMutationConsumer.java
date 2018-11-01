package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.AlsodMutation;
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
  private Consumer<AlsodMutation> alsodMutationConsumer = (mutation) -> {
    Node mutationNode = resolveAlsodMutationNodeFunction.apply(mutation.id());
    lib.novelLabelConsumer.accept(mutationNode, alsAssociatedLabel);
    // add properties
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("MutationName", mutation.mutationName()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("MutationType", mutation.mutationType()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("ChromosomeLocation", mutation.chromosomeLocation()));
    // genetic change
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("SeqChange", mutation.seqChange()));
    // amino acid change
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("AA_Change", mutation.aaChange()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("Exon/Intron", mutation.exonOrIntron()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("HGVS_Nucleotide", mutation.hgvsNucleotide()));
    lib.nodePropertyValueConsumer
        .accept(mutationNode, new Tuple2<>("HGVS_Protein", mutation.hgvsProtein()));
    // establish a relationship to an existing Genetic entity
    if (AlsodMutation.isValidString(mutation.gene())) {
      Tuple3<Label, String, String> tuple3 = new Tuple3<>(geneticEntityLabel, "GeneticEntityId",
          mutation.gene());
      lib.findExistingGraphNodeFunction.apply(tuple3).ifPresent(geneNode ->
          lib.resolveNodeRelationshipFunction
              .apply(new Tuple2<>(mutationNode, geneNode), encodedRelationType)
      );
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(AlsodMutation::parseCSVRecord)
        .forEach(alsodMutationConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(new AlsodMutationConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed alsod mustations file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ALSOD_GENE_MUTATION_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new AlsodMutationConsumer(RunMode.TEST)));
  }
}
