package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.UniProtBlastResult;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import scala.Tuple2;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class UniprotBlastResultConsumer extends GraphDataConsumer {

    public UniprotBlastResultConsumer(RunMode runMode) {
        super(runMode);
    }

    private Consumer<UniProtBlastResult> uniProtBlastResultConsumer = (blastResult) -> {

        Node sourceNode = resolveProteinNodeFunction.apply(blastResult.getSourceUniprotId());
        Node hitNode = resolveProteinNodeFunction.apply(blastResult.getHitUniprotId());
        Tuple2<String, String> keyTuple = new Tuple2<>(blastResult.getSourceUniprotId(),
                blastResult.getHitUniprotId());
        // create or find existing Relationship pair
        Relationship rel = lib.resolveNodeRelationshipFunction
                .apply(new Tuple2<>(sourceNode, hitNode),
                        seqSimRelationType);
        lib.relationshipPropertyValueConsumer.accept(rel,
                new Tuple2<>("BLAST_score", String.valueOf(blastResult.getScore())));
        lib.relationshipPropertyValueConsumer.accept(rel,
                new Tuple2<>("eValue", blastResult.getEValue()));

    };

    @Override
    public void accept(Path path) {
        Preconditions.checkArgument(Files.isRegularFile(path));
        new org.biodatagraphdb.alsdb.util.TsvRecordSplitIteratorSupplier(path, UniProtBlastResult.Companion.getColumnHeadings())
                .get()
                .map(UniProtBlastResult.Companion::parseCSVRecord)
                // filter out self similarity
                .filter(blastRes -> !blastRes.getSourceUniprotId().equalsIgnoreCase(blastRes.getHitUniprotId()))
                .forEach(uniProtBlastResultConsumer);
        lib.shutDown();
    }

    public static void importProdData() {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE.getOptionalPathProperty("SEQ_SIM_FILE")
                .ifPresent(new UniprotBlastResultConsumer(RunMode.PROD));
        AsyncLoggingService.logInfo("processed sequence similarity file : " +
                sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    // main method for stand alone testing using test data
    public static void main(String[] args) {
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("TEST_SEQ_SIM_FILE")
                .ifPresent(path -> new TestGraphDataConsumer()
                        .accept(path, new UniprotBlastResultConsumer(RunMode.TEST)));
    }

}
