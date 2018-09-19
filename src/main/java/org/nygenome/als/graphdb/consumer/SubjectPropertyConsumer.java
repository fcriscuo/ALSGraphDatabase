package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Function;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.LabelTypes;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.StringSubjectProperty;
import scala.Tuple2;

public class SubjectPropertyConsumer extends GraphDataConsumer {


  private Function<String, Node> resolveSubjectNodeFunction = (extSubjectId) -> {
    if (!subjectMap.containsKey(extSubjectId)){
      AsyncLoggingService.logInfo("creating Subject Node for external subject id  " +
         extSubjectId);
      Node subjectNode =  EmbeddedGraph.getGraphInstance()
          .createNode(LabelTypes.Subject);
      nodePropertyValueConsumer.accept(subjectNode, new Tuple2<>("ExternalSubjectId",extSubjectId));
      subjectMap.put(extSubjectId, subjectNode);
      return subjectNode;
    }
    return subjectMap.get(extSubjectId);
  };

  private Function<StringSubjectProperty,Node> resolveSampleNodeFunction = (stringSubjectProperty) -> {
    String externalSampleId = stringSubjectProperty.externalSampleId();
     Node sampleNode = resolveSampleNodeByExternalId(externalSampleId);
     // an existing Sample node may not have had these properties set
      nodePropertyValueConsumer.accept(sampleNode,new Tuple2<>("ExternalSampleId",externalSampleId));
      nodePropertyValueConsumer.accept(sampleNode, new Tuple2<>("SampleType", stringSubjectProperty.sampleType()));
      nodePropertyValueConsumer.accept(sampleNode, new Tuple2<>("AnalyteType", stringSubjectProperty.analyteType()));
    return sampleNode;
  };

  private Consumer<StringSubjectProperty> stringSubjectPropertyConsumer = (subjectProperty) -> {
    Node subjectNode =resolveSubjectNodeFunction.apply(subjectProperty.externalSubjectId());
    nodePropertyValueConsumer.accept(subjectNode, new Tuple2<>(subjectProperty.propertyName(),subjectProperty.propertyValue()));
    Node sampleNode = resolveSampleNodeFunction.apply(subjectProperty);
    // register the relationship between subject and sample if new
    Tuple2<String,String>relationshipKey = new Tuple2<>(subjectProperty.externalSubjectId(), subjectProperty.externalSampleId());
    if( !subjectSampleRelMap.containsKey(relationshipKey)){
      AsyncLoggingService.logInfo("creating Subject-Sample relationship between subject  " +
          subjectProperty.externalSubjectId() +" and sample "
          +subjectProperty.externalSampleId());
      subjectSampleRelMap.put(relationshipKey, subjectNode.createRelationshipTo(sampleNode,
          RelTypes.HAS_SAMPLE));
      // create the inverse relationship
      sampleNode.createRelationshipTo(subjectNode,RelTypes.SAMPLED_FROM);
    }

  };

 /*
 Public Consumer interface method to process the specified file
 as a CSV file containing data that can be mapped to StingSubjectProperty objects
  */
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path);
    new CsvRecordStreamSupplier(path).get()
        .map(StringSubjectProperty::parseCSVRecord)
        .forEach(stringSubjectPropertyConsumer);
  }
}
