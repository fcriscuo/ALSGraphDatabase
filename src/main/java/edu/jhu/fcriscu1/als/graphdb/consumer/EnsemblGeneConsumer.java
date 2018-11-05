package edu.jhu.fcriscu1.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import edu.jhu.fcriscu1.als.graphdb.integration.TestGraphDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.EnsemblGene;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import edu.jhu.fcriscu1.als.graphdb.util.FrameworkPropertyService;
import edu.jhu.fcriscu1.als.graphdb.util.TsvRecordStreamSupplier;
import scala.Tuple2;

/*
A Java Consumer responsible for creating/update Gene Nodes based on
data attributes selected from ensembl's download page
May create/update Gene Ontology Nodes as well
 */
public class EnsemblGeneConsumer extends GraphDataConsumer {


  public EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  private Consumer<EnsemblGene> ensemblGeneConsumer = (gene) -> {
    Node geneNode = resolveEnsemblGeneNodeFunction.apply(gene.ensemblGeneId());
    lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("Chromosome", gene.chromosome()));
    lib.nodeIntegerPropertyValueConsumer
        .accept(geneNode, new Tuple2<>("GeneStart", gene.geneStart()));
    lib.nodeIntegerPropertyValueConsumer.accept(geneNode, new Tuple2<>("GeneEnd", gene.geneEnd()));
    lib.nodeIntegerPropertyValueConsumer.accept(geneNode, new Tuple2<>("Strand", gene.stand()));
    Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(gene.ensemblTranscriptId());
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),
        transcribesRelationType);
    // gene - protein relationship
    if (EnsemblGene.isValidString(gene.uniprotId())) {
      Node proteinNode = resolveProteinNodeFunction.apply(gene.uniprotId());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, proteinNode),
          transcribesRelationType);
    }
    // gene - gene ontology relationship
    Node goNode = resolveGeneOntologyNodeFunction.apply(gene.goEntry());
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, goNode), xrefRelationType);
    // gene - hgnc xref
    if (EnsemblGene.isValidString(gene.hugoSymbol())) {
      Node hgncNode = resolveXrefNode.apply(hgncLabel, gene.hugoSymbol());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, hgncNode), xrefRelationType);
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(EnsemblGene::parseCSVRecord)
        .forEach(ensemblGeneConsumer);
    lib.shutDown();
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_GENE_INFO_FILE")
        .ifPresent(new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl gene info file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_ENSEMBL_GENE_INFO_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
