package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.NeurobankEventTimepoint;
import scala.Tuple2;

/*
Java Consumer responsible for loading subject timepoint events into the database.
ALS study timepoints represent a sequence of visits for an ALS patients
where disease-related measurements (i.e. properties) are completed to
monitor the progression of the disease in an individual patient
This supports temporal longitudinal data
 */
public class NeurobankTimepointEventConsumer extends GraphDataConsumer {

  NeurobankTimepointEventConsumer(RunMode runMode) {super(runMode);}

  private Consumer<NeurobankEventTimepoint> neurobankSubjectTimepointConsumer = (timepoint) -> {
    Node subjectNode = resolveSubjectNodeFunction.apply(
      timepoint.subjectTuple() );
    Node timepointNode = resolveEventTimepointNodeFunction.apply(timepoint.timepointTuple());
    // ensure that these nodes have the ALS and Neurobank labels
    Lists.mutable.of(subjectNode, timepointNode)
        .forEach(annotateNeurobankNodeConsumer);
    // establish a Relationship between these 2 Nodes
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode, timepointNode),
       subjectEventRelationType);
    // add properties to this Relationship
    lib.relationshipPropertyValueConsumer.accept(rel,
        new Tuple2<>("Timepoint",String.valueOf(timepoint.timepoint())));
    lib.relationshipPropertyValueConsumer.accept(rel,
        new Tuple2<>("Interval", String.valueOf(timepoint.timepointInterval())));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(NeurobankEventTimepoint::parseCSVRecord)
        .forEach(neurobankSubjectTimepointConsumer);
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(new NeurobankTimepointEventConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank subject timepoint file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankTimepointEventConsumer(RunMode.TEST)));
  }
}
