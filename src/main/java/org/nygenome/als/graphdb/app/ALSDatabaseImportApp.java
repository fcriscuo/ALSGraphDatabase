
package org.nygenome.als.graphdb.app;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.consumer.AlsGeneConsumer;
import org.nygenome.als.graphdb.consumer.AlsSnpConsumer;
import org.nygenome.als.graphdb.consumer.DrugUniprotInfoConsumer;
import org.nygenome.als.graphdb.consumer.GeneDiseaseAssociationDataConsumer;
import org.nygenome.als.graphdb.consumer.IntactDataConsumer;
import org.nygenome.als.graphdb.consumer.NeurobankCategoryConsumer;
import org.nygenome.als.graphdb.consumer.NeurobankTimepointEventPropertyConsumer;
import org.nygenome.als.graphdb.consumer.NeurobankSubjectPropertyConsumer;
import org.nygenome.als.graphdb.consumer.PathwayInfoConsumer;
import org.nygenome.als.graphdb.consumer.SampleVariantConsumer;
import org.nygenome.als.graphdb.consumer.SubjectPropertyConsumer;
import org.nygenome.als.graphdb.consumer.UniProtValueConsumer;
import org.nygenome.als.graphdb.consumer.VariantDiseaseAssociationDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;

public enum ALSDatabaseImportApp {
  INSTANCE;
  private final GraphDatabaseService graphDb = Suppliers
      .memoize(new GraphDatabaseServiceSupplier(RunMode.PROD)).get();

  public enum RelTypes implements RelationshipType {
      DRUG_TARGET,
    DRUG_ENZYME, DRUG_TRANSPORTER, DRUG_CARRIER

  }

  public enum LabelTypes implements Label {
    Protein,  Gene,  Xref, EnsemblGene, EnsemblTranscript, MolecularFunction,
    BiologicalProcess, CellularComponents, Unknown, Drug_Target, Drug_Enzyme, Drug_Transporter,
    Drug_Carrier,  Variant}



  public static void main(final String[] args) {
    ALSDatabaseImportApp.INSTANCE.createDb();
    ALSDatabaseImportApp.INSTANCE.shutDown();
  }

  public Supplier<Transaction> transactionSupplier = () ->
      graphDb.beginTx();

  void createDb() {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      //Uniprot data
      UniProtValueConsumer.importProdData();
      // Pathway
      PathwayInfoConsumer.importProdData();
      // protein - protein interactions
      IntactDataConsumer.importProdData();
      // ALS genes
      AlsGeneConsumer.importProdData();
      //ALS SNP
      AlsSnpConsumer.importProdData();
      //Subject properties
      SubjectPropertyConsumer.importProdData();
      // Tissue data consumer
      // TODO: limit to ALS genes
      //HumanTissueAtlasDataConsumer.importProdData();
      // Drug data
      DrugUniprotInfoConsumer.importProdData();
      // gene disease associations
      GeneDiseaseAssociationDataConsumer.importProdData();
      // variant disease association
      VariantDiseaseAssociationDataConsumer.importProdData();
      // sample variants
      SampleVariantConsumer.importProdData();
      // neurobank categories
      NeurobankCategoryConsumer.importProdData();
      // neurobank subject properties
      NeurobankSubjectPropertyConsumer.importProdData();
      // neurobank subject timepoints
      NeurobankTimepointEventPropertyConsumer.importProdData();

      stopwatch.stop();
      AsyncLoggingService.logInfo("Creation of the ALS Neo4j database required "
          + stopwatch.elapsed(TimeUnit.SECONDS) + "seconds");
      ;
    } catch (Exception e) {
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    }
  }

  void shutDown() {
    AsyncLoggingService.logInfo("Shutting down database ...");
    graphDb.shutdown();

  }


}
