package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.HgncLocus;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.biodatagraphdb.alsdb.lib.DynamicLabel;
import org.neo4j.graphdb.Node;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class HgncLocusConsumer extends GraphDataConsumer {

    public HgncLocusConsumer(RunMode runMode) {
        super(runMode);
    }

    private BiConsumer<Node, org.biodatagraphdb.alsdb.model.HgncLocus> resolveHgncLocusRelationshipsConsumer = (geNode, hgnc) -> {
        if (HgncLocus.Companion.isValidString(hgnc.getUniprotId())) {
            Node proteinNode = resolveProteinNodeFunction.apply(hgnc.getUniprotId());
            lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, geNode), encodedRelationType);
        }

        if (HgncLocus.Companion.isValidString(hgnc.getEnsemblGeneId())) {
            Node geneNode = resolveGeneticEntityNodeFunction.apply(hgnc.getEnsemblGeneId());
            lib.novelLabelConsumer.accept(geneNode, ensemblLabel);
            lib.novelLabelConsumer.accept(geneNode, geneLabel);
            lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geNode, geneNode), xrefRelationType);
        }
        // HGNC Xref
        registerXrefRelationshipFunction.apply(geNode, hgncLabel, hgnc.getHugoSymbol());
        // Entrez Xref
        if (HgncLocus.Companion.isValidString(hgnc.getEntrezId())) {
            registerXrefRelationshipFunction.apply(geNode, entrezLabel, hgnc.getEntrezId());
        }

        // PubMed Xrefs
        hgnc.getPubMedIdList()
                .forEach(pubMedId -> {
                    registerXrefRelationshipFunction.apply(geNode, pubMedLabel, pubMedId);
                });
        // RefSeq
        if (HgncLocus.Companion.isValidString(hgnc.getRefSeqAccession())) {
            registerXrefRelationshipFunction.apply(geNode, refSeqLabel, hgnc.getRefSeqAccession());
        }
        // CCDS xref
        if (HgncLocus.Companion.isValidString(hgnc.getCcdsId())) {
            registerXrefRelationshipFunction.apply(geNode, ccdsLabel, hgnc.getCcdsId());
        }
        // OMIM
        if (HgncLocus.Companion.isValidString(hgnc.getOmimId())) {
            registerXrefRelationshipFunction.apply(geNode, omimLabel, hgnc.getOmimId());
        }
    };

    /*
    Private Consumer to import data attributes from HGNC
    Currently these data include protein-coding genes and
    various types of RNA
     */
    private Consumer<org.biodatagraphdb.alsdb.model.HgncLocus> hgncLocusConsumer = (hgnc) -> {
        Node geNode = resolveGeneticEntityNodeFunction.apply(hgnc.getId());
        lib.novelLabelConsumer.accept(geNode, hgncLabel);
        lib.novelLabelConsumer.accept(geNode, new DynamicLabel(hgnc.getHgncLocusGroup()));
        lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityType", hgnc.getHgncLocusType()));
        lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityName", hgnc.getHgncName()));
        lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityLocation", hgnc.getHgncLocation()));
        lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("GeneFamily", hgnc.getGeneFamily()));
        // resolve relationships
        resolveHgncLocusRelationshipsConsumer.accept(geNode, hgnc);
    };

    @Override
    public void accept(Path path) {
        Preconditions.checkArgument(path != null);
        new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
                .map(HgncLocus.Companion::parseCSVRecord)
                .filter(org.biodatagraphdb.alsdb.model.HgncLocus::isApprovedLocus)
                .filter(org.biodatagraphdb.alsdb.model.HgncLocus::isApprovedLocusTypeGroup)
                .forEach(hgncLocusConsumer);
        lib.shutDown();
    }

    public static void importProdData() {
        Stopwatch sw = Stopwatch.createStarted();
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("HGNC_COMPLETE_FILE")
                .ifPresent(new HgncLocusConsumer(RunMode.PROD));
        AsyncLoggingService.logInfo("processed HGNC locus file: " +
                sw.elapsed(TimeUnit.SECONDS) + " seconds");
    }

    //main method for stand alone testing
    public static void main(String[] args) {
        org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("TEST_HGNC_COMPLETE_FILE")
                .ifPresent(path -> new TestGraphDataConsumer().accept(path, new HgncLocusConsumer(RunMode.TEST)));
    }
}
