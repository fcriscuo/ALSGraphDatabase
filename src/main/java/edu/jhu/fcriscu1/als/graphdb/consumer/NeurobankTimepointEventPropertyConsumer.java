package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.collections.impl.factory.Lists;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.DynamicRelationshipTypes;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.NeurobankSubjectEventProperty;
import scala.Tuple2;

public class
NeurobankTimepointEventPropertyConsumer extends GraphDataConsumer {

  public NeurobankTimepointEventPropertyConsumer(RunMode runMode) {
    super(runMode);
  }

  private final RelationshipType eventValueSubjectRelType = new DynamicRelationshipTypes(
      "HAS_PROPERTY");
  private final RelationshipType eventValueEventRelType = new DynamicRelationshipTypes(
      "HAS_EVENT_OCCURRANCE"
  );
  private final RelationshipType eventTimepointRelType = new DynamicRelationshipTypes(
      "OCCURRED_AT"
  );

  private Consumer<NeurobankSubjectEventProperty> neurobankSubjectEventPropertyConsumer =
      (property) -> {

        // create the chain of nodes
        Node subjectNode = resolveSubjectNodeFunction.apply(property.subjectTuple());
        Node timepointNode = resolveEventTimepointNodeFunction.apply(property.timepointTuple());
        Node eventNode = resolveSubjectEventNodeFunction.apply(property.subjectEventTuple());
        // event property node
        Node eventPropertyNode = resolveSubjectEventPropertyNodeFunction
            .apply(property.eventPropertyId());
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyForm", property.formName()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyCode", property.propertyCode()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyName", property.propertyName()));
        lib.nodePropertyValueConsumer.accept(eventPropertyNode,
            new Tuple2<>("Value", property.propertyValue()));
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
        Node propertyCategoryNode = resolveCategoryNode.apply(property.propertyCategory());
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyNode, propertyCategoryNode),
                categorizesRelType);
        Node eventCategoryNode = resolveCategoryNode.apply(property.eventCategory());
        lib.resolveNodeRelationshipFunction.apply( new Tuple2<>(eventNode,eventCategoryNode),
            categorizesRelType);
      };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectEventProperty::parseCSVRecord)
        .forEach(neurobankSubjectEventPropertyConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent(new NeurobankTimepointEventPropertyConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank subject event property file : " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // import  these data individually
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_NEUROBANK_SUBJECT_TIMEPOINT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer()
                .accept(path, new NeurobankTimepointEventPropertyConsumer(RunMode.TEST)));
  }

}
