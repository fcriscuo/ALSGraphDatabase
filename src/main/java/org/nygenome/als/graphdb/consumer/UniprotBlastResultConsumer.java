package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordSplitIteratorSupplier;
import org.nygenome.als.graphdb.value.UniProtBlastResult;
import scala.Tuple2;

public class UniprotBlastResultConsumer extends GraphDataConsumer {

  public UniprotBlastResultConsumer(RunMode runMode) {
    super(runMode);
  }

  private Consumer<UniProtBlastResult> uniProtBlastResultConsumer = (blastResult) -> {

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
    new TsvRecordSplitIteratorSupplier(path, UniProtBlastResult.columnHeadings())
        .get()
        .map(UniProtBlastResult::parseCSVRecord)
        // filter out self similarity
        .filter(blastRes -> !blastRes.sourceUniprotId().equalsIgnoreCase(blastRes.hitUniprotId()))
        .forEach(uniProtBlastResultConsumer);
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SEQ_SIM_FILE")
        .ifPresent(new UniprotBlastResultConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed sequence similarity file : " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // main method for stand alone testing using test data
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_SEQ_SIM_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new UniprotBlastResultConsumer(RunMode.TEST)));
  }

}
