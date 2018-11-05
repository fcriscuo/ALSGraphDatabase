
package edu.jhu.fcriscu1.als.graphdb.app;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import edu.jhu.fcriscu1.als.graphdb.consumer.AlsGeneConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.AlsSnpConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.DrugUniprotInfoConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.GeneDiseaseAssociationDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.IntactDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.NeurobankCategoryConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.NeurobankTimepointEventPropertyConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.NeurobankSubjectPropertyConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.SampleVariantConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.ShutdownConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.SubjectPropertyConsumer;
import edu.jhu.fcriscu1.als.graphdb.consumer.VariantDiseaseAssociationDataConsumer;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;

public enum ALSDatabaseImportApp {
  INSTANCE;
//  private final GraphDatabaseService graphDb = Suppliers
//      .memoize(new GraphDatabaseServiceSupplier(RunMode.PROD)).get();

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
  }

  void createDb() {
    try {
      Stopwatch stopwatch = Stopwatch.createStarted();
      //Uniprot data
      //UniProtValueConsumer.importProdData();
      // Pathway
      //PathwayInfoConsumer.importProdData();
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
      // shutdown the database
      ShutdownConsumer.importProdData();

      stopwatch.stop();
      AsyncLoggingService.logInfo("Creation of the ALS Neo4j database required "
          + stopwatch.elapsed(TimeUnit.SECONDS) + "seconds");
      ;
    } catch (Exception e) {
      AsyncLoggingService.logError(e.getMessage());
      e.printStackTrace();
    }
  }


}
