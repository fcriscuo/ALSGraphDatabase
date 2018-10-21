
package org.nygenome.als.graphdb.app;

import com.google.common.base.Stopwatch;
import com.google.common.base.Suppliers;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.nygenome.als.graphdb.consumer.NeurobankSubjectEventPropertyConsumer;
import org.nygenome.als.graphdb.consumer.NeurobankSubjectPropertyConsumer;
import org.nygenome.als.graphdb.consumer.PathwayInfoConsumer;
import org.nygenome.als.graphdb.consumer.SampleVariantConsumer;
import org.nygenome.als.graphdb.consumer.SubjectPropertyConsumer;
import org.nygenome.als.graphdb.consumer.UniProtValueConsumer;
import org.nygenome.als.graphdb.consumer.VariantDiseaseAssociationDataConsumer;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;


public enum ALSDatabaseImportApp {
  INSTANCE;
  private final Path DB_PATH = Paths
      .get(FrameworkPropertyService.INSTANCE.getStringProperty("neo4j.db.path"));
  private GraphDatabaseService graphDb = Suppliers
      .memoize(new GraphDatabaseServiceSupplier(DB_PATH)).get();

  public enum RelTypes implements RelationshipType {
      DRUG_TARGET,
    DRUG_ENZYME, DRUG_TRANSPORTER, DRUG_CARRIER

  }

  public enum LabelTypes implements Label {
    Protein,  Gene,  Xref, EnsemblGene, EnsemblTranscript, MolecularFunction,
    BiologicalProcess, CellularComponents, Unknown, Drug_Target, Drug_Enzyme, Drug_Transporter,
    Drug_Carrier,  Variant}

  // convenience method to satisfy legacy usages
  public static GraphDatabaseService getGraphInstance() {
    return ALSDatabaseImportApp.INSTANCE.graphDb;
  }

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
      UniProtValueConsumer.importData();
      // Pathway
      PathwayInfoConsumer.importData();
      // protein - protein interactions
      IntactDataConsumer.importData();
      // ALS genes
      AlsGeneConsumer.importData();
      //ALS SNP
      AlsSnpConsumer.importData();
      //Subject properties
      SubjectPropertyConsumer.importData();
      // Tissue data consumer
      // TODO: limit to ALS genes
      //HumanTissueAtlasDataConsumer.importData();
      // Drug data
      DrugUniprotInfoConsumer.importData();
      // gene disease associations
      GeneDiseaseAssociationDataConsumer.importData();
      // variant disease association
      VariantDiseaseAssociationDataConsumer.importData();
      // sample variants
      SampleVariantConsumer.importData();
      // neurobank categories
      NeurobankCategoryConsumer.importData();
      // neurobank subject properties
      NeurobankSubjectPropertyConsumer.importData();
      // neurobank subject timepoints
      NeurobankSubjectEventPropertyConsumer.importData();

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
