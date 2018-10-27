package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.DynamicLabel;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.StringUtils;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.HgncLocus;
import scala.Tuple2;

public class HgncLocusConsumer  extends GraphDataConsumer{

  public HgncLocusConsumer(RunMode runMode) { super(runMode);}


  private BiConsumer<Node,HgncLocus> resolveHgncLocusRelationshipsConsumer = (geNode, hgnc) -> {
    if (HgncLocus.isValidString(hgnc.uniprotId())) {
      Node proteinNode = resolveProteinNodeFunction.apply(hgnc.uniprotId());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode,geNode),encodedRelationType );
    }

    if (HgncLocus.isValidString(hgnc.ensemblGeneId())) {
      Node geneNode = resolveGeneticEntityNodeFunction.apply(hgnc.ensemblGeneId());
      lib.novelLabelConsumer.accept(geneNode,ensemblLabel);
      lib.novelLabelConsumer.accept(geneNode, geneLabel);
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(geNode,geneNode),xrefRelationType);
    }
    // HGNC Xref
    registerXrefRelationshipFunction.apply(geNode, hgncLabel, hgnc.hugoSymbol());
    // Entrez Xref
    if( HgncLocus.isValidString(hgnc.entrezId())){
      registerXrefRelationshipFunction.apply(geNode,entrezLabel, hgnc.entrezId());
    }

    // PubMed Xrefs
    StringUtils.convertToJavaString(hgnc.pubMedIdList())
        .forEach(pubMedId ->{
          registerXrefRelationshipFunction.apply(geNode, pubMedLabel, pubMedId);
        });
    // RefSeq
    if(HgncLocus.isValidString(hgnc.refSeqAccession())) {
      registerXrefRelationshipFunction.apply(geNode,refSeqLabel,hgnc.refSeqAccession());
    }
    // CCDS xref
    if(HgncLocus.isValidString(hgnc.ccdsId())) {
      registerXrefRelationshipFunction.apply(geNode,ccdsLabel,hgnc.ccdsId());
    }
    // OMIM
    if(HgncLocus.isValidString(hgnc.omimId())){
      registerXrefRelationshipFunction.apply(geNode, omimLabel, hgnc.omimId());
    }
  };

  /*
  Private Consumer to import data attributes from HGNC
  Currently these data include protein-coding genes and
  various types of RNA
   */
  private Consumer<HgncLocus> hgncLocusConsumer = (hgnc) -> {
    Node geNode = resolveGeneticEntityNodeFunction.apply(hgnc.id());
    lib.novelLabelConsumer.accept(geNode,hgncLabel);
    lib.novelLabelConsumer.accept(geNode,new DynamicLabel(hgnc.hgncLocusGroup()));
    lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityType",hgnc.hgncLocusType()));
    lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityName",hgnc.hgncName()));
    lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("EntityLocation",hgnc.hgncLocation()));
    lib.nodePropertyValueConsumer.accept(geNode, new Tuple2<>("GeneFamily",hgnc.geneFamily()));
    // resolve relationships
    resolveHgncLocusRelationshipsConsumer.accept(geNode, hgnc);

  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new TsvRecordStreamSupplier(path).get()
        .map(HgncLocus::parseCSVRecord)
        .filter(HgncLocus::isApprovedLocus)
        .filter(HgncLocus::isApprovedLocusTypeGroup)
        .forEach(hgncLocusConsumer);
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("HGNC_COMPLETE_FILE")
        .ifPresent(new HgncLocusConsumer(RunMode.PROD));
    AsyncLoggingService.logInfo("processed HGNC locus file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  //main method for stand alone testing
  public static void main(String[] args) {
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_HGNC_COMPLETE_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new HgncLocusConsumer(RunMode.TEST)));
  }
}
