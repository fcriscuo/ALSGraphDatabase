package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

public class UniprotBlastResultConsumer extends GraphDataConsumer {

  public UniprotBlastResultConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  private Consumer<org.biodatagraphdb.alsdb.value.UniProtBlastResult> uniProtBlastResultConsumer = (blastResult) -> {

      Node sourceNode = resolveProteinNodeFunction.apply(blastResult.sourceUniprotId());
      Node hitNode = resolveProteinNodeFunction.apply(blastResult.hitUniprotId());
      Tuple2<String, String> keyTuple = new Tuple2<>(blastResult.sourceUniprotId(),
          blastResult.hitUniprotId());
      // create or find existing Relationship pair
      Relationship rel = lib.resolveNodeRelationshipFunction
          .apply(new Tuple2<>(sourceNode, hitNode),
              seqSimRelationType);
      lib.relationshipPropertyValueConsumer.accept(rel,
          new Tuple2<>("BLAST_score", String.valueOf(blastResult.score())));
      lib.relationshipPropertyValueConsumer.accept(rel,
          new Tuple2<>("eValue", blastResult.eValue()));

  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.isRegularFile(path));
    new org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier(path, org.biodatagraphdb.alsdb.value.UniProtBlastResult.columnHeadings())
        .get()
        .map(org.biodatagraphdb.alsdb.value.UniProtBlastResult::parseCSVRecord)
        // filter out self similarity
        .filter(blastRes -> !blastRes.sourceUniprotId().equalsIgnoreCase(blastRes.hitUniprotId()))
        .forEach(uniProtBlastResultConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SEQ_SIM_FILE")
        .ifPresent(new UniprotBlastResultConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed sequence similarity file : " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // main method for stand alone testing using test data
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_SEQ_SIM_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new UniprotBlastResultConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }

}
