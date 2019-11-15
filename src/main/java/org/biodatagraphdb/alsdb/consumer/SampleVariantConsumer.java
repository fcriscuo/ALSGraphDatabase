package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;
/*
Java Consumer responsible for importing sample variant data
Creates new Sample variant nodes
A SampleVariantNode will be specific for each gene in each sample
and a List of variants as a CSV String
 */

public class SampleVariantConsumer extends GraphDataConsumer{

  public SampleVariantConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

 private Consumer<org.biodatagraphdb.alsdb.value.SampleVariantSummary> sampleVariantSummaryConsumer = (svc) -> {

   // this is the only class that will create SampleVariant nodes and
   // their relationships
   Node geneNode = resolveEnsemblGeneNodeFunction.apply(svc.ensemblGeneId());
   Node sampleNode = resolveSampleNodeFunction.apply(svc.extSampleId());
   Node sampleVariantNode = resolveSampleVariantNode.apply(svc);
   if (lib.getIsAlsAssociatedPredicate().test(geneNode)) {
     lib.getNovelLabelConsumer().accept(sampleVariantNode, alsAssociatedLabel);
     lib.getNovelLabelConsumer().accept(sampleNode, alsAssociatedLabel);
   }
   // create sample <-> sampleVariant relationship
   Relationship sampleToSampleVariantRel = lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(sampleNode,sampleVariantNode),
       new org.biodatagraphdb.alsdb.util.DynamicRelationshipTypes("sample_variant"));
   lib.getSetRelationshipIntegerProperty().accept(sampleToSampleVariantRel, new Tuple2<>("VariantCount", svc.numVariants()));

   lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(sampleVariantNode,geneNode),
      encodedRelationType);
 };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(null != path
        && Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier(path, org.biodatagraphdb.alsdb.value.SampleVariantSummary.columnHeadings())
        .get()
        .map(org.biodatagraphdb.alsdb.value.SampleVariantSummary::parseCSVRecord)
        .forEach(sampleVariantSummaryConsumer);
    lib.shutDown();

  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("SAMPLE_VARIANT_SUMUMMARY_FILE")
        .ifPresent(new SampleVariantConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed complete sample variant file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_SAMPLE_VARIANT_SUMUMMARY_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new SampleVariantConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }

}
