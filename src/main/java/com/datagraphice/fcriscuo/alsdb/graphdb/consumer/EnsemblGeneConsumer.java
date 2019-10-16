package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import edu.jhu.fcriscu1.als.graphdb.value.EnsemblGene;
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

  private Consumer<EnsemblGene> ensemblGeneConsumer = (gene) -> {
    Node geneNode = resolveEnsemblGeneNodeFunction.apply(gene.ensemblGeneId());
    lib.getNodePropertyValueConsumer().accept(geneNode, new Tuple2<>("Chromosome", gene.chromosome()));
    lib.getNodeIntegerPropertyValueConsumer()
        .accept(geneNode, new Tuple2<>("GeneStart", gene.geneStart()));
    lib.getNodeIntegerPropertyValueConsumer().accept(geneNode, new Tuple2<>("GeneEnd", gene.geneEnd()));
    lib.getNodeIntegerPropertyValueConsumer().accept(geneNode, new Tuple2<>("Strand", gene.stand()));
    Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(gene.ensemblTranscriptId());
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geneNode, transcriptNode),
        transcribesRelationType);
    // gene - protein relationship
    if (EnsemblGene.isValidString(gene.uniprotId())) {
      Node proteinNode = resolveProteinNodeFunction.apply(gene.uniprotId());
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geneNode, proteinNode),
          transcribesRelationType);
    }
    // gene - gene ontology relationship
    Node goNode = resolveGeneOntologyNodeFunction.apply(gene.goEntry());
    lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geneNode, goNode), xrefRelationType);
    // gene - hgnc xref
    if (EnsemblGene.isValidString(gene.hugoSymbol())) {
      Node hgncNode = resolveXrefNode.apply(hgncLabel, gene.hugoSymbol());
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geneNode, hgncNode), xrefRelationType);
    }
  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier(path).get()
        .map(EnsemblGene::parseCSVRecord)
        .forEach(ensemblGeneConsumer);
    lib.shutDown();
  }

  public static void importData() {
    Stopwatch sw = Stopwatch.createStarted();
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("ENSEMBL_GENE_INFO_FILE")
        .ifPresent(new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed ensembl gene info file: " +
        sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  public static void main(String[] args) {
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_ENSEMBL_GENE_INFO_FILE")
        .ifPresent(path -> new TestGraphDataConsumer()
            .accept(path, new EnsemblGeneConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
