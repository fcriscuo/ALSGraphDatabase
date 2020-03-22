package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.ProActAdverseEvent;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.Tuple3;

public class ProActAdverseEventConsumer extends GraphDataConsumer {

  public ProActAdverseEventConsumer(RunMode runMode) {super(runMode);}

  /*
  Private Function to create the hierarchy of event categories
   */
  private Function<org.biodatagraphdb.alsdb.model.ProActAdverseEvent,Node>  resolveEventCategoryNodeFunction = (event) -> {
    Node socNode = lib.resolveGraphNodeFunction.apply(new Tuple3<>(
        systemOrganClassLabel, "SystemOrganClassCode",event.getSocCode()));
    lib.nodePropertyValueConsumer.accept(socNode, new Tuple2<>("SystemOrganClass", event.getSystemOrganClass()));
    lib.nodePropertyValueConsumer.accept(socNode, new Tuple2<>("SOCAbbreviation", event.getSocAbbreviation()));

    Node hiLevelGrpTermNode = lib.resolveGraphNodeFunction.apply( new Tuple3<>(
        highLevelGroupTermLabel,"HighLevelGroupTerm",event.getHighLevelGroupTerm()));
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(hiLevelGrpTermNode,socNode),childRelationType);

    Node hiLevelTermNode = lib.resolveGraphNodeFunction.apply( new Tuple3<>(
        highLevelTermLabel,"HighLevelTerm",event.getHighLevelTerm()));
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(hiLevelTermNode,hiLevelGrpTermNode),childRelationType);

    Node preferredTermNode = lib.resolveGraphNodeFunction.apply( new Tuple3<>(
        preferredTermLabel,"PreferredTerm",event.getPreferredTerm()));
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(preferredTermNode,hiLevelTermNode),childRelationType);

//    Node lowestLevelTermNode = lib.resolveGraphNodeFunction.apply( new Tuple3<>(
//        lowestLevelTermLabel,"LowestLevelTerm",event.lowestLevelTerm()));
//    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(lowestLevelTermNode,preferredTermNode),childRelationType);
    return preferredTermNode;

  };

  private Consumer<org.biodatagraphdb.alsdb.model.ProActAdverseEvent> proactAdverseEventConsumer = (event) -> {
    Node lowestLevelTermNode = resolveEventCategoryNodeFunction.apply(event);
    Node subjectNode  = resolveSubjectNodeFunction.apply(event.getSubjectTuple());
    lib.novelLabelConsumer.accept(subjectNode, alsAssociatedLabel);
    lib.novelLabelConsumer.accept(subjectNode,proactLabel);
    // establish  relationship between subject and adverse event
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(subjectNode, lowestLevelTermNode),
        categorizesRelType);
    // add properties to the relationship
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Severity",event.getSeverity()));
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Outcome",event.getOutcome()));
    lib.setRelationshipIntegerProperty.accept(rel, new Tuple2<>("StartDateDelta",event.getStartDateDelta()));
    lib.setRelationshipIntegerProperty.accept(rel, new Tuple2<>("EndDateDelta",event.getEndDateDelta()));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier(path).get()
        .map(ProActAdverseEvent.Companion::parseCSVRecord)
        .forEach(proactAdverseEventConsumer);
    lib.shutDown();
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(new ProActAdverseEventConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed proact adverse event file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new ProActAdverseEventConsumer(RunMode.TEST)));
  }
}
