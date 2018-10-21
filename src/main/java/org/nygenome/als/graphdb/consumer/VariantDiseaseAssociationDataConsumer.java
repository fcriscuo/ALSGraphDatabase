package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.VariantDiseaseAssociation;
import scala.Tuple2;

public class VariantDiseaseAssociationDataConsumer extends GraphDataConsumer{

  private Consumer<VariantDiseaseAssociation> variantDiseaseAssociationConsumer = (snp) ->{
    Node diseaseNode = resolveDiseaseNodeFunction.apply(snp.diseaseId());
    // set or reset disease name
    lib.nodePropertyValueConsumer.accept(diseaseNode, new Tuple2<>("DiseaseName", snp.diseaseName()));
    Node snpNode = resolveDiseaseNodeFunction.apply(snp.snpId());
    // create  relationship between snp & disease
    Relationship rel = lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(snpNode, diseaseNode),
        implicatedInRelationType);
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("ConfidenceLevel",String.valueOf(snp.score())));
    lib.relationshipPropertyValueConsumer.accept(rel, new Tuple2<>("Reference",snp.source()));
  };
  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new TsvRecordStreamSupplier(path)
        .get()
        .map(VariantDiseaseAssociation::parseCSVRecord)
        .forEach(variantDiseaseAssociationConsumer);
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(new VariantDiseaseAssociationDataConsumer());
    AsyncLoggingService.logInfo("processed variant disease associaton file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  // main method for stand alone testing
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path,new VariantDiseaseAssociationDataConsumer()));
  }

}
