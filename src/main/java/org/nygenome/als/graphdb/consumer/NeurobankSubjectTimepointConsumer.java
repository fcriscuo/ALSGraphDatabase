package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.NeurobankSubjectTimepoint;
import scala.Tuple2;

/*
Java Consumer responsible for loading subject timepoints into the database.
ALS study timepoints represent a sequence of visits for an ALS patients
where disease-related measurements (i.e. properties) are completed to
monitor the progression of the disease in an individual patient
This supports temporal longitudinal data
This class will create Relationships between Subject nodes and StudyTimepoint nodes
These Realyionships will have properties containing the specific timepoint  value for that
Subject as well as the interval from the previous timepoint for that Subject
StudyTimepoints will have a Relationship to a previous StudyTimepoint establishing
an order for their occurrence
 */
public class NeurobankSubjectTimepointConsumer extends GraphDataConsumer {

  private Consumer<NeurobankSubjectTimepoint> neurobankSubjectTimepointConsumer = (timepoint) -> {
    Node subjectNode = resolveSubjectNodeFunction.apply(timepoint.subjectGuid());
    // ensure that this subject has the ALS and Neurobank labels
    lib.novelLabelConsumer.accept(subjectNode, neurobankLabel);
    lib.novelLabelConsumer.accept(subjectNode, alsAssociatedLabel);
    Node timepointNode = resolveStudyTimepointNode.apply(timepoint.id());
    // ensure that this timepoint has the ALS and Neurobank labels
    lib.novelLabelConsumer.accept(timepointNode, neurobankLabel);
    lib.novelLabelConsumer.accept(timepointNode, alsAssociatedLabel);
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
        .map(NeurobankSubjectTimepoint::parseCSVRecord)
        .forEach(neurobankSubjectTimepointConsumer);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(new NeurobankSubjectTimepointConsumer());
    AsyncLoggingService.logInfo("processed neurobank subject timepoint file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_TIMEPOINT_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer().accept(path, new NeurobankSubjectTimepointConsumer()));
  }
}
