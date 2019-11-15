package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
Consumer that will import data mined from ensembl
related to gene associated with the ALS phenotype.
The genes imported from these data will constitute the
"white list" for ALS genes
Additional ALS genes may be added to this collection manually

 */
public class AlsGeneConsumer extends GraphDataConsumer{

  public AlsGeneConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  // keep track of what genes have been processed to avoid
  // repetitive property setting
  private Set<String> processedAlsGeneSet = Sets.mutable.empty();

  private Consumer<org.biodatagraphdb.alsdb.value.EnsemblAlsGene> alsGeneConsumer = (alsGene -> {
     Node geneNode = resolveGeneticEntityNodeFunction.apply(alsGene.ensemblGeneId());
     if(!processedAlsGeneSet.contains(alsGene.ensemblGeneId())) {
       // label this genetic entity as a Gene

       // add ALS label if these nodes have not been already labeled
       lib.getNovelLabelConsumer().accept(geneNode, alsAssociatedLabel);
       // set/reset gene properties
       lib.getNodePropertyValueConsumer()
           .accept(geneNode, new Tuple2<>("Chromosome", alsGene.chromosome()));
       lib.getNodeIntegerPropertyValueConsumer()
           .accept(geneNode, new Tuple2<>("GeneStart", alsGene.geneStart()));
       lib.getNodeIntegerPropertyValueConsumer()
           .accept(geneNode, new Tuple2<>("GeneEnd", alsGene.geneEnd()));
       lib.getNodePropertyValueConsumer().accept(geneNode, new Tuple2<>("Strand", alsGene.strand()));
       processedAlsGeneSet.add(alsGene.ensemblGeneId());
       AsyncLoggingService.logInfo("Added new ALS-associated gene: " +alsGene.hugoName());
     }
     Node transcriptNode =  resolveGeneticEntityNodeFunction.apply(alsGene.ensemblTranscriptId());
    lib.getNovelLabelConsumer().accept(transcriptNode, alsAssociatedLabel);
     Node proteinNode = resolveProteinNodeFunction.apply(alsGene.uniprotId());
    lib.getNovelLabelConsumer().accept(proteinNode, alsAssociatedLabel);
    // define relationships: gene - transcript - protein - gene
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geneNode, transcriptNode),
        transcribesRelationType );
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(proteinNode, transcriptNode),
        geneticEntityRelationType);
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(proteinNode, geneNode),
        geneticEntityRelationType);
  });

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.EnsemblAlsGene::parseCSVRecord)
        .forEach(alsGeneConsumer);
    lib.shutDown();
    AsyncLoggingService.logInfo("ALS-associated gene count = " +processedAlsGeneSet.size());
  }


  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(new AlsGeneConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl als genes file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }

  // Test mode
  public static void main(String[] args) {
   org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer().accept(path, new AlsGeneConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
