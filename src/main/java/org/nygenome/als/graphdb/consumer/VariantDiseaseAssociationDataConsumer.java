package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.EmbeddedGraphApp;
import org.nygenome.als.graphdb.app.EmbeddedGraphApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
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
    // create bi-directional relationship between snp & disease
    Tuple2<String,String> relTuple = new Tuple2<>(snp.snpId(), snp.diseaseId());
   lib.createBiDirectionalRelationship(snpNode,diseaseNode, relTuple,
        snpDiseaseRelMap,  RelTypes.IMPLICATED_IN,RelTypes.ASSOCIATED_VARIANT
        );
    Transaction tx = EmbeddedGraphApp.INSTANCE.transactionSupplier.get();
    try {
      snpDiseaseRelMap.get(relTuple).setProperty("ConfidenceLevel",snp.score());
      snpDiseaseRelMap.get(relTuple).setProperty("Reference",snp.source());
      tx.success();
    } catch (Exception e) {
      e.printStackTrace();
      tx.failure();
    } finally {
      tx.close();
    }
  };


  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(Files.exists(path, LinkOption.NOFOLLOW_LINKS));
    new TsvRecordStreamSupplier(path)
        .get()
        .map(VariantDiseaseAssociation::parseCSVRecord)
        .forEach(variantDiseaseAssociationConsumer);
  }

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE.getOptionalPathProperty("VARIANT_DISEASE_ASSOC_DISGENET_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path,new VariantDiseaseAssociationDataConsumer()));
  }
}
