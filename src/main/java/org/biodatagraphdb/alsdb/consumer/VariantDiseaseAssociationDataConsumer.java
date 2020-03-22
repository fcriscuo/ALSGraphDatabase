package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.VariantDiseaseAssociation;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

public class VariantDiseaseAssociationDataConsumer extends GraphDataConsumer{
  public VariantDiseaseAssociationDataConsumer(RunMode runMode) {super(runMode);}

  private Consumer<org.biodatagraphdb.alsdb.model.VariantDiseaseAssociation> variantDiseaseAssociationConsumer = (snp) ->{
    Node diseaseNode = resolveDiseaseNodeFunction.apply(snp.getDiseaseId());
    // set or reset disease name
    lib.nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseName", snp.getDiseaseName()));
    Node snpNode = resolveSnpNodeFunction.apply(snp.getSnpId());
    // create  relationship between snp & disease
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(snpNode, diseaseNode),
        implicatedInRelationType);
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("ConfidenceLevel",String.valueOf(snp.getScore())));
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Reference",snp.getSource()));
  };
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path)
        .get()
        .map(VariantDiseaseAssociation.Companion::parseCSVRecord)
        .forEach(variantDiseaseAssociationConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new VariantDiseaseAssociationDataConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed variant disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  // main method for stand alone testing
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("TEST_VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path,new VariantDiseaseAssociationDataConsumer(RunMode.TEST)));
  }

}
