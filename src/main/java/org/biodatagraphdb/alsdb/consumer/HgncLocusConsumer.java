package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.util.DynamicLabel;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import edu.jhu.fcriscu1.als.graphdb.util.StringUtils;
import scala.Tuple2;

public class HgncLocusConsumer  extends GraphDataConsumer{

  public HgncLocusConsumer(GraphDatabaseServiceSupplier.RunMode runMode) { super(runMode);}


  private BiConsumer<Node, org.biodatagraphdb.alsdb.value.HgncLocus> resolveHgncLocusRelationshipsConsumer = (geNode, hgnc) -> {
    if (org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.uniprotId())) {
      Node proteinNode = resolveProteinNodeFunction.apply(hgnc.uniprotId());
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(proteinNode,geNode),encodedRelationType );
    }

    if (org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.ensemblGeneId())) {
      Node geneNode = resolveGeneticEntityNodeFunction.apply(hgnc.ensemblGeneId());
      lib.getNovelLabelConsumer().accept(geneNode,ensemblLabel);
      lib.getNovelLabelConsumer().accept(geneNode, geneLabel);
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(geNode,geneNode),xrefRelationType);
    }
    // HGNC Xref
    registerXrefRelationshipFunction.apply(geNode, hgncLabel, hgnc.hugoSymbol());
    // Entrez Xref
    if( org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.entrezId())){
      registerXrefRelationshipFunction.apply(geNode,entrezLabel, hgnc.entrezId());
    }

    // PubMed Xrefs
    StringUtils.convertToJavaString(hgnc.pubMedIdList())
        .forEach(pubMedId ->{
          registerXrefRelationshipFunction.apply(geNode, pubMedLabel, pubMedId);
        });
    // RefSeq
    if(org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.refSeqAccession())) {
      registerXrefRelationshipFunction.apply(geNode,refSeqLabel,hgnc.refSeqAccession());
    }
    // CCDS xref
    if(org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.ccdsId())) {
      registerXrefRelationshipFunction.apply(geNode,ccdsLabel,hgnc.ccdsId());
    }
    // OMIM
    if(org.biodatagraphdb.alsdb.value.HgncLocus.isValidString(hgnc.omimId())){
      registerXrefRelationshipFunction.apply(geNode, omimLabel, hgnc.omimId());
    }
  };

  /*
  Private Consumer to import data attributes from HGNC
  Currently these data include protein-coding genes and
  various types of RNA
   */
  private Consumer<org.biodatagraphdb.alsdb.value.HgncLocus> hgncLocusConsumer = (hgnc) -> {
    Node geNode = resolveGeneticEntityNodeFunction.apply(hgnc.id());
    lib.getNovelLabelConsumer().accept(geNode,hgncLabel);
    lib.getNovelLabelConsumer().accept(geNode,new DynamicLabel(hgnc.hgncLocusGroup()));
    lib.getNodePropertyValueConsumer().accept(geNode, new Tuple2<>("EntityType",hgnc.hgncLocusType()));
    lib.getNodePropertyValueConsumer().accept(geNode, new Tuple2<>("EntityName",hgnc.hgncName()));
    lib.getNodePropertyValueConsumer().accept(geNode, new Tuple2<>("EntityLocation",hgnc.hgncLocation()));
    lib.getNodePropertyValueConsumer().accept(geNode, new Tuple2<>("GeneFamily",hgnc.geneFamily()));
    // resolve relationships
    resolveHgncLocusRelationshipsConsumer.accept(geNode, hgnc);

  };

  @Override
  public void accept(Path path) {
    Preconditions.checkArgument(path != null);
    new org.biodatagraphdb.alsdb.util.TsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.HgncLocus::parseCSVRecord)
        .filter(org.biodatagraphdb.alsdb.value.HgncLocus::isApprovedLocus)
        .filter(org.biodatagraphdb.alsdb.value.HgncLocus::isApprovedLocusTypeGroup)
        .forEach(hgncLocusConsumer);
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("HGNC_COMPLETE_FILE")
        .ifPresent(new HgncLocusConsumer(GraphDatabaseServiceSupplier.RunMode.PROD));
    AsyncLoggingService.logInfo("processed HGNC locus file: " +
        sw.elapsed(TimeUnit.SECONDS) +" seconds");
  }
  //main method for stand alone testing
  public static void main(String[] args) {
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("TEST_HGNC_COMPLETE_FILE")
        .ifPresent(path -> new TestGraphDataConsumer().accept(path, new HgncLocusConsumer(GraphDatabaseServiceSupplier.RunMode.TEST)));
  }
}
