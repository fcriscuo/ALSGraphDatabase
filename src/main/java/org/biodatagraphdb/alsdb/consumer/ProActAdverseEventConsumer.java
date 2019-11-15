package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
import scala.Tuple3;

public class ProActAdverseEventConsumer extends GraphDataConsumer {

  public ProActAdverseEventConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

  /*
  Private Function to create the hierarchy of event categories
   */
  private Function<org.biodatagraphdb.alsdb.value.ProActAdverseEvent,Node>  resolveEventCategoryNodeFunction = (event) -> {
    Node socNode = lib.getResolveGraphNodeFunction().apply(new Tuple3<>(
        systemOrganClassLabel, "SystemOrganClassCode",event.socCode()));
    lib.getNodePropertyValueConsumer().accept(socNode, new Tuple2<>("SystemOrganClass", event.systemOrganClass()));
    lib.getNodePropertyValueConsumer().accept(socNode, new Tuple2<>("SOCAbbreviation", event.socAbbreviation()));

    Node hiLevelGrpTermNode = lib.getResolveGraphNodeFunction().apply( new Tuple3<>(
        highLevelGroupTermLabel,"HighLevelGroupTerm",event.highLevelGroupTerm()));
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(hiLevelGrpTermNode,socNode),childRelationType);

    Node hiLevelTermNode = lib.getResolveGraphNodeFunction().apply( new Tuple3<>(
        highLevelTermLabel,"HighLevelTerm",event.highLevelTerm()));
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(hiLevelTermNode,hiLevelGrpTermNode),childRelationType);

    Node preferredTermNode = lib.getResolveGraphNodeFunction().apply( new Tuple3<>(
        preferredTermLabel,"PreferredTerm",event.preferredTerm()));
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(preferredTermNode,hiLevelTermNode),childRelationType);

//    Node lowestLevelTermNode = lib.resolveGraphNodeFunction.apply( new Tuple3<>(
//        lowestLevelTermLabel,"LowestLevelTerm",event.lowestLevelTerm()));
//    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(lowestLevelTermNode,preferredTermNode),childRelationType);
    return preferredTermNode;

  };

  private Consumer<org.biodatagraphdb.alsdb.value.ProActAdverseEvent> proactAdverseEventConsumer = (event) -> {
    Node lowestLevelTermNode = resolveEventCategoryNodeFunction.apply(event);
    Node subjectNode  = resolveSubjectNodeFunction.apply(event.subjectTuple());
    lib.getNovelLabelConsumer().accept(subjectNode, alsAssociatedLabel);
    lib.getNovelLabelConsumer().accept(subjectNode,proactLabel);
    // establish  relationship between subject and adverse event
    Relationship rel = lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(subjectNode, lowestLevelTermNode),
        categorizesRelType);
    // add properties to the relationship
    lib.getRelationshipPropertyValueConsumer().accept(rel, new Tuple2<>("Severity",event.severity()));
    lib.getRelationshipPropertyValueConsumer().accept(rel, new Tuple2<>("Outcome",event.outcome()));
    lib.getSetRelationshipIntegerProperty().accept(rel, new Tuple2<>("StartDateDelta",event.startDateDelta()));
    lib.getSetRelationshipIntegerProperty().accept(rel, new Tuple2<>("EndDateDelta",event.endDateDelta()));
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.ProActAdverseEvent::parseCSVRecord)
        .forEach(proactAdverseEventConsumer);
    lib.shutDown();
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(new ProActAdverseEventConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed proact adverse event file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalResourcePath("TEST_PROACT_ADVERSE_EVENT_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new ProActAdverseEventConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
