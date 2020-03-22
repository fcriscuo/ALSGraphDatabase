package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.RnaTpmGene;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.neo4j.graphdb.Node;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/*
Public Consumer responsible for creating graph nodes and relationships from
data contained in a TSV specified TSV file
Because it is anticipated that these files will be extremely large,
a streaming utility based on a Splititerator is used
 */
public class RnaTpmGeneConsumer extends GraphDataConsumer {

    public RnaTpmGeneConsumer(RunMode runMode) {
        super(runMode);
    }

    /*
    private Consumer to process a RnaTpmGene object
    Will create an RnaTpmGene Node  and if necessary an EnsembleGene Node
    and/or a HugoGene Node
    It will create Relationships between all three Node types
     */
    private Consumer<org.biodatagraphdb.alsdb.model.RnaTpmGene> rnaTpmGeneConsumer = (tpm) -> {
        Node rnaNode = resolveRnaTpmGeneNode.apply(tpm);
        // Optional.get is OK because we've already filtered on it presence
        // and this is a private method only called from processing the  stream of TSV records
        Node ensemblGeneNode = resolveEnsemblGeneNodeFunction
                .apply(tpm.getUniProtMapping().get().getEnsemblGeneId());
        Node ensemblTranscriptNode = resolveEnsemblTranscriptNodeFunction
                .apply(tpm.getUniProtMapping().get().getEnsemblTranscriptId());
        Node proteinNode = resolveProteinNodeFunction
                .apply(tpm.getUniProtMapping().get().getUniProtId());
        // establish a relationship between the RNA node and the protein node
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, rnaNode),
                expressionLevelRelationType);
        // TODO: add xref to HUGO
//      lib.createBiDirectionalRelationship(proteinNode, hugoGeneNode,
//          new Tuple2<>(tpm.uniProtMapping().get().uniProtId(),
//              tpm.uniProtMapping().get().geneSymbol()), proteinXrefRelMap, RelTypes.REFERENCES,
//          RelTypes.REFERENCES);
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, ensemblGeneNode),
                encodedRelationType);
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, ensemblTranscriptNode),
                encodedRelationType);
    };

    @Override
    public void accept(Path path) {
        Preconditions.checkArgument(null != path
                && Files.exists(path, LinkOption.NOFOLLOW_LINKS));
        new org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier(path, RnaTpmGene.Companion.getColumnHeadings())
                .get()
                .map(RnaTpmGene.Companion::parseCsvRecordFunction)
                .filter(rnaTpmGene -> rnaTpmGene.getUniProtMapping().isPresent())
                .filter(rtg -> rtg.getTpm() > 0.0D)
                .forEach(rnaTpmGeneConsumer);
        lib.shutDown();
    }

    public static void importProdData() {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("RNA_TPM_GENE_FILE")
                .ifPresent(new RnaTpmGeneConsumer(RunMode.PROD));
        AsyncLoggingService.logInfo("read rna tpm  data: " +
                sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    /*
    main method for standalone testing of this Consumer
    Uses a truncated version of the actual source file
     */
    public static void main(String[] args) {
        // use generic TestGraphDataConsumer to test
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("TEST_RNA_TPM_GENE_FILE")
                .ifPresent(path ->
                        new TestGraphDataConsumer().accept(path, new RnaTpmGeneConsumer(RunMode.TEST)));
    }


}
