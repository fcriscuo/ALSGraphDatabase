package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.EnsemblAlsGene;

import org.biodatagraphdb.alsdb.service.graphdb.*;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.service.property.FrameworkPropertiesService;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphdb.Node;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
Consumer that will import data mined from ensembl
related to gene associated with the ALS phenotype.
The genes imported from these data will constitute the
"white list" for ALS genes
Additional ALS genes may be added to this collection manually

 */
public class AlsGeneConsumer extends GraphDataConsumer{

  public AlsGeneConsumer(org.biodatagraphdb.alsdb.service.graphdb.RunMode runMode) {
    super(runMode);
  }

  // keep track of what genes have been processed to avoid
  // repetitive property setting
  private Set<String> processedAlsGeneSet = Sets.mutable.empty();

  private Consumer<org.biodatagraphdb.alsdb.model.EnsemblAlsGene> alsGeneConsumer = (alsGene -> {
     Node geneNode = resolveGeneticEntityNodeFunction.apply(alsGene.getEnsemblGeneId());
     if(!processedAlsGeneSet.contains(alsGene.getEnsemblGeneId())) {
       // label this genetic entity as a Gene

       // add ALS label if these nodes have not been already labeled
       lib.novelLabelConsumer.accept(geneNode, alsAssociatedLabel);
       // set/reset gene properties
       lib.nodePropertyValueConsumer
           .accept(geneNode, new Tuple2<>("Chromosome", alsGene.getChromosome()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("GeneStart", alsGene.getGeneStart()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("GeneEnd", alsGene.getGeneEnd()));
       lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("Strand", alsGene.getStrand()));
       processedAlsGeneSet.add(alsGene.getEnsemblGeneId());
       AsyncLoggingService.logInfo("Added new ALS-associated gene: " +alsGene.getHugoName());
     }
     Node transcriptNode =  resolveGeneticEntityNodeFunction.apply(alsGene.getEnsemblTranscriptId());
    lib.novelLabelConsumer.accept(transcriptNode, alsAssociatedLabel);
     Node proteinNode = resolveProteinNodeFunction.apply(alsGene.getUniprotId());
    lib.novelLabelConsumer.accept(proteinNode, alsAssociatedLabel);
    // define relationships: gene - transcript - protein - gene
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),
        transcribesRelationType );
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, transcriptNode),
        geneticEntityRelationType);
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, geneNode),
        geneticEntityRelationType);
  });

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(EnsemblAlsGene.Companion::parseCSVRecord)
        .forEach(alsGeneConsumer);
    lib.shutDown();
    AsyncLoggingService.logInfo("ALS-associated gene count = " +processedAlsGeneSet.size());
  }


  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
     FrameworkPropertiesService.INSTANCE.resolvePropertyAsPathOption("ENSEMBL_ALS_GENES_FILE")
            .map(path-> new AlsGeneConsumer(RunMode.PROD));
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(new AlsGeneConsumer(org.biodatagraphdb.alsdb.service.graphdb.RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl als genes file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // Test mode
  public static void main(String[] args) {
   org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new AlsGeneConsumer(org.biodatagraphdb.alsdb.service.graphdb.RunMode.TEST)));
  }
}
