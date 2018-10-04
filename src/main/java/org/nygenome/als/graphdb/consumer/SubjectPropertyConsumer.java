package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.StringSubjectProperty;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

public class SubjectPropertyConsumer extends GraphDataConsumer {

  private FunctionLib lib = new FunctionLib();


  private Function<StringSubjectProperty, Node> resolveSampleNodeFunction = (stringSubjectProperty) -> {
    String externalSampleId = stringSubjectProperty.externalSampleId();
    Node sampleNode = resolveSampleNodeByExternalIdFunction.apply(externalSampleId);
    // an existing Sample node may not have had these properties set
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("ExternalSampleId", externalSampleId));
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("SampleType", stringSubjectProperty.sampleType()));
    lib.nodePropertyValueConsumer
        .accept(sampleNode, new Tuple2<>("AnalyteType", stringSubjectProperty.analyteType()));
    return sampleNode;
  };

  private Consumer<StringSubjectProperty> stringSubjectPropertyConsumer = (subjectProperty) -> {
    Node subjectNode = resolveSubjectNodeFunction.apply(subjectProperty.externalSubjectId());
    lib.nodePropertyValueConsumer.accept(subjectNode,
        new Tuple2<>(subjectProperty.propertyName(), subjectProperty.propertyValue()));
    Node sampleNode = resolveSampleNodeFunction.apply(subjectProperty);
    // register the relationship between subject and sample if new
    Tuple2<String, String> relationshipKey = new Tuple2<>(subjectProperty.externalSubjectId(),
        subjectProperty.externalSampleId());
    if (!subjectSampleRelMap.containsKey(relationshipKey)) {
      AsyncLoggingService.logInfo("creating Subject-Sample relationship between subject  " +
          subjectProperty.externalSubjectId() + " and sample "
          + subjectProperty.externalSampleId());
      lib.createBiDirectionalRelationship(subjectNode, sampleNode, relationshipKey,
          subjectSampleRelMap,
          RelTypes.HAS_SAMPLE, RelTypes.SAMPLED_FROM);
    }

  };

  private Predicate<StringSubjectProperty> alsPredicate = (ssp) ->
      !ssp.externalSubjectId().toUpperCase().startsWith("TCGA");

  /*
  Public Consumer interface method to process the specified file
  as a CSV file containing data that can be mapped to StringSubjectProperty objects
   */
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path);
    new TsvRecordStreamSupplier(path,StringSubjectProperty.columnHeadings()).get()
        .map(StringSubjectProperty::parseCSVRecord)
        .filter(alsPredicate)
        .forEach(stringSubjectPropertyConsumer);
  }

  public static void main(String... args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SUBJECT_PROPERTY_FILE")
        .ifPresent(new SubjectPropertyConsumer());
  }
}
