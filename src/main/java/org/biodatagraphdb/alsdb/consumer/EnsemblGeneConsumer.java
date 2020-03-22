package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.EnsemblGene;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
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

    public EnsemblGeneConsumer(RunMode runMode) {
        super(runMode);
    }

    private Consumer<EnsemblGene> ensemblGeneConsumer = (gene) -> {
        Node geneNode = resolveEnsemblGeneNodeFunction.apply(gene.getEnsemblGeneId());
        lib.nodePropertyValueConsumer.accept(geneNode, new Tuple2<>("Chromosome", gene.getChromosome()));
        lib.nodeIntegerPropertyValueConsumer
                .accept(geneNode, new Tuple2<>("GeneStart", gene.getGeneStart()));
        lib.nodeIntegerPropertyValueConsumer.accept(geneNode, new Tuple2<>("GeneEnd", gene.getGeneEnd()));
        lib.nodeIntegerPropertyValueConsumer.accept(geneNode, new Tuple2<>("Strand", gene.getStrand()));
        Node transcriptNode = resolveEnsemblTranscriptNodeFunction.apply(gene.getEnsemblTranscriptId());
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, transcriptNode),
                transcribesRelationType);
        // gene - protein relationship
        if (EnsemblGene.Companion.isValidString(gene.getUniprotId())) {
            Node proteinNode = resolveProteinNodeFunction.apply(gene.getUniprotId());
            lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, proteinNode),
                    transcribesRelationType);
        }
        // gene - gene ontology relationship
        Node goNode = resolveGeneOntologyNodeFunction.apply(gene.getGoEntry());
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, goNode), xrefRelationType);
        // gene - hgnc xref
        if (EnsemblGene.Companion.isValidString(gene.getHugoSymbol())) {
            Node hgncNode = resolveXrefNode.apply(hgncLabel, gene.getHugoSymbol());
            lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geneNode, hgncNode), xrefRelationType);
        }
    };

    @Override
    public void accept(Path path) {
        Preconditions.checkArgument(path != null);
        new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
                .map(EnsemblGene.Companion::parseCSVRecord)
                .forEach(ensemblGeneConsumer);
        lib.shutDown();
    }

    public static void importData() {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("ENSEMBL_GENE_INFO_FILE")
                .ifPresent(new EnsemblGeneConsumer(RunMode.PROD));
        AsyncLoggingService.logInfo("processed ensembl gene info file: " +
                sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    public static void main(String[] args) {
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("TEST_ENSEMBL_GENE_INFO_FILE")
                .ifPresent(path -> new TestGraphDataConsumer()
                        .accept(path, new EnsemblGeneConsumer(RunMode.TEST)));
    }
}
