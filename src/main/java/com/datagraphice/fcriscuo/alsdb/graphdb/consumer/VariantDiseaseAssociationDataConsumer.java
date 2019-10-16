package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.VariantDiseaseAssociation;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

public class VariantDiseaseAssociationDataConsumer extends GraphDataConsumer{
  public VariantDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}

  private Consumer<VariantDiseaseAssociation> variantDiseaseAssociationConsumer = (snp) ->{
    Node diseaseNode = resolveDiseaseNodeFunction.apply(snp.diseaseId());
    // set or reset disease name
    lib.getNodePropertyValueConsumer().accept(diseaseNode, new Tuple2<>("DiseaseName", snp.diseaseName()));
    Node snpNode = resolveSnpNodeFunction.apply(snp.snpId());
    // create  relationship between snp & disease
    Relationship rel = lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(snpNode, diseaseNode),
        implicatedInRelationType);
    lib.getRelationshipPropertyValueConsumer().accept(rel, new Tuple2<>("ConfidenceLevel",String.valueOf(snp.score())));
    lib.getRelationshipPropertyValueConsumer().accept(rel, new Tuple2<>("Reference",snp.source()));
  };
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier(path)
        .get()
        .map(VariantDiseaseAssociation::parseCSVRecord)
        .forEach(variantDiseaseAssociationConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new VariantDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed variant disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  // main method for stand alone testing
  public static void main(String[] args) {
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path,new VariantDiseaseAssociationDataConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }

}
