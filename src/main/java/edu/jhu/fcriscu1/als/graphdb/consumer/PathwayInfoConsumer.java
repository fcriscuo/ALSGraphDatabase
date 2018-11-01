package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.util.TsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.Pathway;
import scala.Tuple2;
import javax.annotation.Nonnull;
import java.nio.file.Path;


public class PathwayInfoConsumer extends GraphDataConsumer implements Consumer<Path>{

  public PathwayInfoConsumer(RunMode runMode) {super(runMode);}


  /*
  Private Consumer to resolve (i.e. find/create) a Pathway Node
  and establish a protein->pathway Relationship if novel
   */
  private Consumer<Pathway> pathwayConsumer = (pathway)-> {
    Node pathwayNode = resolvePathwayNodeFunction.apply(pathway.id());
    // if this is the first time we've seen this Pathway we need to set the
    // pathway name
      lib.nodePropertyValueConsumer
          .accept(pathwayNode, new Tuple2<>("Pathway", pathway.eventName()));
    Node proteinNode = resolveProteinNodeFunction.apply(pathway.uniprotId());
      lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, pathwayNode),pathwayRelationshipType );
  };

/*
Public Consumer get method to process the UniProt-Reactome tsv file at a
specified path
 */

    @Override
    public void accept(@Nonnull Path path) {
        new TsvRecordStreamSupplier(path).get().map(Pathway::parseCSVRecord)
                .filter(pathway -> !pathway.uniprotId().startsWith("A") )
               // .filter(pathway-> Pathway.isHuman(pathway.species()) )// only human entries
                .filter(pathway-> pathway.species().equalsIgnoreCase("Homo sapiens") )
                .forEach(pathwayConsumer);
        lib.shutDown();
    }


    public static void importProdData() {
      Stopwatch sw = Stopwatch.createStarted();
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
          .ifPresent(new PathwayInfoConsumer(RunMode.PROD));
      AsyncLoggingService.logInfo("read pathway data: " +
          sw.elapsed(TimeUnit.SECONDS) +" seconds");
    }

    public static void main(String... args) {
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
          .ifPresent(path->
              new TestGraphDataConsumer().accept(path,new PathwayInfoConsumer(RunMode.TEST)));

    }

}
