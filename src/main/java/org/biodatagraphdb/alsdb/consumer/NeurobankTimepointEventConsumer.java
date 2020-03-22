package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.NeurobankEventTimepoint;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
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

  private Consumer<org.biodatagraphdb.alsdb.model.NeurobankEventTimepoint> neurobankSubjectTimepointConsumer = (timepoint) -> {
    Node subjectNode = resolveSubjectNodeFunction.apply(
      timepoint.getSubjectIdPair() );
    Node timepointNode = resolveEventTimepointNodeFunction.apply(timepoint.getTimepointIdPair());
    // ensure that these nodes have the ALS and Neurobank labels
    Lists.mutable.of(subjectNode, timepointNode)
        .forEach(annotateNeurobankNodeConsumer);
    // establish a Relationship between these 2 Nodes
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode, timepointNode),
       subjectEventRelationType);
    // add properties to this Relationship
    lib.relationshipPropertyValueConsumer.accept(rel,
        new Tuple2<>("Timepoint",String.valueOf(timepoint.getTimepoint())));
    lib.relationshipPropertyValueConsumer.accept(rel,
        new Tuple2<>("Interval", String.valueOf(timepoint.getTimepointInterval())));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(NeurobankEventTimepoint.Companion::parseCSVRecord)
        .forEach(neurobankSubjectTimepointConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(new NeurobankTimepointEventConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank subject timepoint file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankTimepointEventConsumer(RunMode.TEST)));
  }
}
