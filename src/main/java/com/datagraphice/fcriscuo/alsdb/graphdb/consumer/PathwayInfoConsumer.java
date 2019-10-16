package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import edu.jhu.fcriscu1.als.graphdb.value.Pathway;
import scala.Tuple2;
import javax.annotation.Nonnull;
import java.nio.file.Path;


public class PathwayInfoConsumer extends GraphDataConsumer implements Consumer<Path>{

  public PathwayInfoConsumer(com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode runMode) {super(runMode);}


  /*
  Private Consumer to resolve (i.e. find/create) a Pathway Node
  and establish a protein->pathway Relationship if novel
   */
  private Consumer<Pathway> pathwayConsumer = (pathway)-> {
    Node pathwayNode = resolvePathwayNodeFunction.apply(pathway.id());
    // if this is the first time we've seen this Pathway we need to set the
    // pathway name
      lib.getNodePropertyValueConsumer()
          .accept(pathwayNode, new Tuple2<>("Pathway", pathway.eventName()));
    Node proteinNode = resolveProteinNodeFunction.apply(pathway.uniprotId());
      lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(proteinNode, pathwayNode),pathwayRelationshipType );
  };

/*
Public Consumer get method to process the UniProt-Reactome tsv file at a
specified path
 */

    @Override
    public void accept(@Nonnull Path path) {
        new com.datagraphice.fcriscuo.alsdb.graphdb.util.TsvRecordStreamSupplier(path).get().map(Pathway::parseCSVRecord)
                .filter(pathway -> !pathway.uniprotId().startsWith("A") )
               // .filter(pathway-> Pathway.isHuman(pathway.species()) )// only human entries
                .filter(pathway-> pathway.species().equalsIgnoreCase("Homo sapiens") )
                .forEach(pathwayConsumer);
        lib.shutDown();
    }


    public static void importProdData() {
      Stopwatch sw = Stopwatch.createStarted();
      com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
          .ifPresent(new PathwayInfoConsumer(com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode.PROD));
      AsyncLoggingService.logInfo("read pathway data: " +
          sw.elapsed(TimeUnit.SECONDS) +" seconds");
    }

    public static void main(String... args) {
      com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
          .ifPresent(path->
              new com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer().accept(path,new PathwayInfoConsumer(com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode.TEST)));

    }

}
