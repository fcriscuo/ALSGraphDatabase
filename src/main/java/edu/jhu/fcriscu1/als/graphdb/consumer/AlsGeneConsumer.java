package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.EnsemblAlsGene;
import scala.Tuple2;

/*
Consume that will import data mined from ensembl
related to gene associated with the ALS phenotype.
The genes imported from these data will constitute thw
"white list" for ALS genes
Additional ALS genes may be added to this collection manually

 */
public class AlsGeneConsumer extends GraphDataConsumer{

  public AlsGeneConsumer(RunMode runMode) {
    super(runMode);
  }

  // keep track of what genes have been processed to avoid
  // repetitive property setting
  private Set<String> processedAlsGeneSet = Sets.mutable.empty();

  private Consumer<EnsemblAlsGene> alsGeneConsumer = (alsGene -> {
     Node geneNode = resolveGeneticEntityNodeFunction.apply(alsGene.ensemblGeneId());
     if(!processedAlsGeneSet.contains(alsGene.ensemblGeneId())) {
       // label this genetic entity as a Gene

       // add ALS label if these nodes have not been already labeled
       lib.novelLabelConsumer.accept(geneNode, alsAssociatedLabel);
       // set/reset gene properties
       lib.nodePropertyValueConsumer
           .accept(geneNode, new Tuple2<>("Chromosome", alsGene.chromosome()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("GeneStart", alsGene.geneStart()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("GeneEnd", alsGene.geneEnd()));
       lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("Strand", alsGene.strand()));
       processedAlsGeneSet.add(alsGene.ensemblGeneId());
       AsyncLoggingService.logInfo("Added new ALS-associated gene: " +alsGene.hugoName());
     }
     Node transcriptNode =  resolveGeneticEntityNodeFunction.apply(alsGene.ensemblTranscriptId());
    lib.novelLabelConsumer.accept(transcriptNode, alsAssociatedLabel);
     Node proteinNode = resolveProteinNodeFunction.apply(alsGene.uniprotId());
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
    new TsvRecordStreamSupplier(path).get()
        .map(EnsemblAlsGene::parseCSVRecord)
        .forEach(alsGeneConsumer);
    lib.shutDown();
    AsyncLoggingService.logInfo("ALS-associated gene count = " +processedAlsGeneSet.size());
  }


  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(new AlsGeneConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl als genes file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // Test mode
  public static void main(String[] args) {
   FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new AlsGeneConsumer(RunMode.TEST)));
  }
}
