package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.EnsemblAlsSnp;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.neo4j.graphdb.Node;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
Java Consumer responsible for importing SNPs for
ALS-associated genes from data downloaded from ensembl
It is assumed that this consumer will be invoked AFTER
the AlsGeneConsumer, so protein, gene, and transcript nodes will
have been created
 */
public class AlsSnpConsumer  extends GraphDataConsumer{

  public AlsSnpConsumer(RunMode runMode){
    super(runMode);
  }

  private Consumer<org.biodatagraphdb.alsdb.model.EnsemblAlsSnp> alsSnpConsumer = (snp)-> {
    Node snpNode = resolveSnpNodeFunction.apply(snp.getVariantId());
    // add ALS label if these nodes have not been already labeled
    lib.novelLabelConsumer.accept(snpNode, alsAssociatedLabel);
    // set/reset SNP properties
    lib.nodeIntegerPropertyValueConsumer.accept(snpNode, new Tuple2<>("DistanceToTranscript", snp.getDistance()));
    lib.nodePropertyValueConsumer.accept(snpNode, new Tuple2<>("VariantAlleles", snp.getAlleleVariation()));
    // this will create a Transcript Node if run in stand-alone test mode
    Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(snp.getEnsemblTranscriptId());
    // establish a relationship between transcript and snp
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(transcriptNode,snpNode) ,
        geneticEntityRelationType);
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(EnsemblAlsSnp.Companion::parseCSVRecord)
        .forEach(alsSnpConsumer);
    lib.shutDown();
  }
  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_SNP_FILE")
        .ifPresent(new AlsSnpConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl als snp file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // stand alone test
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_ENSEMBL_ALS_SNP_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new AlsSnpConsumer(RunMode.TEST)));
  }
}
