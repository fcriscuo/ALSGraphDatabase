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

public class NeurobankSubjectEventPropertyConsumer extends GraphDataConsumer {

  public NeurobankSubjectEventPropertyConsumer(RunMode runMode) {
    super(runMode);
  }

  private final RelationshipType subjectEventValueRelType = new DynamicRelationshipTypes(
      "EVENT_MEASREMENT");
  private final RelationshipType subjectEventCategoryRelType = new DynamicRelationshipTypes(
      "CATEGORIZES");
  private final RelationshipType eventValueEventPropertyRelType = new DynamicRelationshipTypes(
      "CATEGORIZES");
  private final RelationshipType eventValueTimepointRelType = new DynamicRelationshipTypes(
      "OCCURRED_AT");

  private Consumer<NeurobankSubjectEventProperty> neurobankSubjectEventPropertyConsumer =
      (property) -> {

        Node subjectNode = resolveSubjectNodeFunction.apply(property.subjectGuid());
        Node timepointNode = resolveStudyTimepointNode.apply(property.timepointName());
        // event property node
        Node eventPropertyNode = resolveSubjectEventPropertyNodeFunction
            .apply(property.eventPropertyId());
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyForm", property.formName()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyCode", property.propertyCode()));
        lib.nodePropertyValueConsumer
            .accept(eventPropertyNode, new Tuple2<>("PropertyName", property.propertyName()));
        // event property value
        Node eventPropertyValueNode = resolveSubjectEventPropertyValueNodeFunction
            .apply(property.eventValueId());
        lib.nodePropertyValueConsumer.accept(eventPropertyValueNode,
            new Tuple2<>("PropertyValue", property.propertyValue()));
        // ensure that Neurobank-associated nodes are annotated
        Lists.mutable.of(subjectNode, timepointNode, eventPropertyNode, eventPropertyValueNode)
            .forEach(annotateNeurobankNodeConsumer);
        // complete Node Relationships
        // subject <-> property value
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(subjectNode, eventPropertyValueNode), subjectEventValueRelType
            );
        // event property <-> category
        Node eventCategoryNode = resolveCategoryNode.apply(property.propertyCategory());
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyNode, eventCategoryNode),
                subjectEventCategoryRelType);
        // event property value <-> event property
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyValueNode, eventPropertyNode),
                eventValueEventPropertyRelType);
        // event property value <-> timepoint
        lib.resolveNodeRelationshipFunction
            .apply(new Tuple2<>(eventPropertyValueNode, timepointNode)
                , eventValueTimepointRelType);
      };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(NeurobankSubjectEventProperty::parseCSVRecord)
        .forEach(neurobankSubjectEventPropertyConsumer);
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent(new NeurobankSubjectEventPropertyConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed neurobank subject event property file : " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  // import  these data individually
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("NEUROBANK_SUBJECT_EVENT_PROPERTY_FILE")
        .ifPresent(
            path -> new TestGraphDataConsumer()
                .accept(path, new NeurobankSubjectEventPropertyConsumer(RunMode.TEST)));
  }

}
