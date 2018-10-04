package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.eclipse.collections.impl.factory.Sets;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.EnsemblAlsGene;
import org.nygenome.als.graphdb.util.DynamicLabel;
import scala.Tuple2;

/*
Consume that will import data mined from ensembl
related to gene associated with the ALS phenotype.
The genes imported from these data will constitute thw
"white list" for ALS genes
Additional ALS genes may be added to this collection manually

 */
public class AlsGeneConsumer extends GraphDataConsumer{
  private final Label  alsLabel = new DynamicLabel("ALS");
  // keep track of what genes have been processed to avoid
  // repetitive property setting
  private Set<String> processedAlsGeneSet = Sets.mutable.empty();

  private Consumer<EnsemblAlsGene> alsGeneConsumer = (alsGene -> {
     Node geneNode = resolveGeneticEntityNodeFunction.apply(alsGene.ensemblGeneId());
     if(!processedAlsGeneSet.contains(alsGene.ensemblGeneId())) {
       // add ALS label if these nodes have not been already labeled
       lib.novelLabelConsumer.accept(geneNode, alsLabel);
       // set/reset gene properties
       lib.nodePropertyValueConsumer
           .accept(geneNode, new Tuple2<>("chromosome", alsGene.chromosome()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("start pos", alsGene.geneStart()));
       lib.nodeIntegerPropertyValueConsumer
           .accept(geneNode, new Tuple2<>("end end", alsGene.geneEnd()));
       lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("strand", alsGene.strand()));
       processedAlsGeneSet.add(alsGene.ensemblGeneId());
     }
     Node transcriptNode =  resolveGeneticEntityNodeFunction.apply(alsGene.ensemblTranscriptId());
    lib.novelLabelConsumer.accept(transcriptNode,alsLabel);
     Node proteinNode = resolveProteinNodeFunction.apply(alsGene.uniprotId());
    lib.novelLabelConsumer.accept(proteinNode,alsLabel);
    // define relationships: gene <-> transcript <-> protein <-> gene
    lib.createBiDirectionalRelationship(geneNode,transcriptNode,
        new Tuple2<>(alsGene.ensemblGeneId(), alsGene.ensemblTranscriptId()),
        geneTranscriptMap, RelTypes.TRANSCRIBES, RelTypes.ENCODED_BY
        );
    lib.createBiDirectionalRelationship(proteinNode,transcriptNode,
        new Tuple2<>(alsGene.uniprotId(), alsGene.ensemblTranscriptId()),
        proteinGeneticEntityMap, RelTypes.ASSOCIATED_GENETIC_ENTITY,RelTypes.ASSOCIATED_PROTEIN);
    lib.createBiDirectionalRelationship(proteinNode,geneNode, new Tuple2<>(alsGene.uniprotId(),alsGene.ensemblTranscriptId()),
        proteinGeneticEntityMap,RelTypes.ASSOCIATED_GENETIC_ENTITY,RelTypes.EXPRESSED_PROTEIN);

  });

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(EnsemblAlsGene::parseCSVRecord)
        .forEach(alsGeneConsumer);
  }
  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(new AlsGeneConsumer());
    AsyncLoggingService.logInfo("processed ensembl als genes file : " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_ALS_GENES_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new AlsGeneConsumer()));
  }
}
