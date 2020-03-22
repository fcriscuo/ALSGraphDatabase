package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.NeurobankSubjectEventProperty;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.graphdb.Node;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class
NeurobankTimepointEventPropertyConsumer extends GraphDataConsumer {

  public NeurobankTimepointEventPropertyConsumer(RunMode runMode) {
    super(runMode);
  }

  private Consumer<org.biodatagraphdb.alsdb.model.NeurobankSubjectEventProperty> neurobankSubjectEventPropertyConsumer =
      (property) -> {

        // create the chain of nodes
        Node subjectNode = resolveSubjectNodeFunction.apply(property.getSubjectIdPair());
        Node timepointNode = resolveEventTimepointNodeFunction.apply(property.getTimepointIdPair());
        Node eventNode = resolveSubjectEventNodeFunction.apply(property.getSubjectEventNamePair());
        // event property node
        Node eventPropertyNode = resolveSubjectEventPropertyNodeFunction
            .apply(property.getEventPropertyId());
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyForm", property.getFormName()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyCode", property.getPropertyCode()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyName", property.getPropertyName()));
        lib.nodePropertyValueConsumer.accept(eventPropertyNode,
            new Tuple2<>("Value", property.getPropertyValue()));
        // ensure that Neurobank-associated nodes are annotated
        Lists.mutable.of(subjectNode, timepointNode, eventNode, eventPropertyNode)
            .forEach(annotateNeurobankNodeConsumer);
        // complete Node Relationships
        // property value -> subject
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyNode, subjectNode), eventValueSubjectRelType
            );
        // property value -> event
        lib.resolveNodeRelationshipFunction.apply(
            new Tuple2(eventPropertyNode,eventNode), eventValueEventRelType
        );

        // event  <-> timepoint
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventNode, timepointNode), eventTimepointRelType
            );
        // associate nodes with categories
        Node propertyCategoryNode = resolveCategoryNode.apply(property.getPropertyCategory());
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyNode, propertyCategoryNode),
                categorizesRelType);
        Node eventCategoryNode = resolveCategoryNode.apply(property.getEventCategory());
        lib.resolveNodeRelationshipFunction.apply( new Tuple2<>(eventNode,eventCategoryNode),
            categorizesRelType);
      };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectEventProperty.Companion::parseCSVRecord)
        .forEach(neurobankSubjectEventPropertyConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent(new NeurobankTimepointEventPropertyConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank subject event property file : " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // import  these data individually
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer()
                .accept(path, new NeurobankTimepointEventPropertyConsumer(RunMode.TEST)));
  }

}
