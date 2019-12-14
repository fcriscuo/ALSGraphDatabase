package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.neo4j.graphdb.Node;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/*
A Java Consumer responsible for creating/update Gene Nodes based on
data attributes selected from ensembl's download page
May create/update Gene Ontology Nodes as well
 */
public class EnsemblGeneConsumer extends GraphDataConsumer {


  public EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode runMode) {
    super(runMode);
  }

  private Consumer<org.biodatagraphdb.alsdb.value.EnsemblGene> ensemblGeneConsumer = (gene) -> {
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
    if (org.biodatagraphdb.alsdb.value.EnsemblGene.isValidString(gene.uniprotId())) {
      Node proteinNode = resolveProteinNodeFunction.apply(gene.uniprotId());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, proteinNode),
          transcribesRelationType);
    }
    // gene - gene ontology relationship
    Node goNode = resolveGeneOntologyNodeFunction.apply(gene.goEntry());
    lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, goNode), xrefRelationType);
    // gene - hgnc xref
    if (org.biodatagraphdb.alsdb.value.EnsemblGene.isValidString(gene.hugoSymbol())) {
      Node hgncNode = resolveXrefNode.apply(hgncLabel, gene.hugoSymbol());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, hgncNode), xrefRelationType);
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.EnsemblGene::parseCSVRecord)
        .forEach(ensemblGeneConsumer);
    lib.shutDown();
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_GENE_INFO_FILE")
        .ifPresent(new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl gene info file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_ENSEMBL_GENE_INFO_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
