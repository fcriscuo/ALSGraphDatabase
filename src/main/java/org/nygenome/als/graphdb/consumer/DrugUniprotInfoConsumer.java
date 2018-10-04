package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Stopwatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.UniProtDrug;
import java.nio.file.Path;
import scala.Tuple2;

/*
A Consumer responsible for processing the UniProt-DrugBank files
for: drug targets, drug enzymes, drug carriers, and drug transporters
Will create new Protein nodes if uniprotId value is novel
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer  {
    private static final Logger log = Logger.getLogger(DrugUniprotInfoConsumer.class);
    private final RelTypes eRelType;
    /*
    DRUG_TARGET,
		DRUG_ENZYME, DRUG_TRANSPORTER, DRUG_CARRIER
     */
   private void addDrugTypeLabel(Node node)  {
       String drugInteractionType = eRelType.name();
       switch (drugInteractionType) {
           case ("DRUG_TARGET"):
               node.addLabel(LabelTypes.Drug_Target);
               break;
           case ("DRUG_ENZYME"):
               node.addLabel(LabelTypes.Drug_Enzyme);
               break;
           case ("DRUG_TRANSPORTER"):
               node.addLabel(LabelTypes.Drug_Transporter);
               break;
           case ("DRUG_CARRIER"):
               node.addLabel(LabelTypes.Drug_Carrier);
               break;
           default:
               AsyncLoggingService.logError(drugInteractionType + " is an invalid drug type");
       }

       }


    private BiConsumer<RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
        = (drugRelType, drug) -> {
        String uniprotId = drug.uniprotId();
        // check if the protein node exists
        try ( Transaction tx = ALSDatabaseImportApp.INSTANCE.transactionSupplier.get()) {
            Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
            // label the Protein Node with its drug characteristic
            addDrugTypeLabel(proteinNode);
            drug.drugIdList().forEach((id) -> {
                Node drugNode = resolveDrugBankNode.apply(id);
                proteinDrugRelMap.put(new Tuple2<>(uniprotId, id),
                    proteinNode.createRelationshipTo(drugNode, drugRelType));
            });
            tx.success();
        } catch (Exception e) {
            AsyncLoggingService.logError(e.getMessage());
            e.printStackTrace();
        }
    };

/*
Sample line from csv
    ID,Name,Gene Name,GenBank Protein ID,GenBank Gene ID,UniProt ID,Uniprot Title,PDB ID,GeneCard ID,GenAtlas ID,HGNC ID,Species,Drug IDs
P45059,Peptidoglycan synthase FtsI,ftsI,1574687,L42023,P45059,FTSI_HAEIN,"",,,,Haemophilus influenzae (strain ATCC 51907 / DSM 11121 / KW20 / Rd),DB00303
     */

    public DrugUniprotInfoConsumer(ALSDatabaseImportApp.RelTypes eRelType) {
        this.eRelType = eRelType;
    }

    @Override
    public void accept(Path path) {
        new CsvRecordStreamSupplier(path).get()
            .map(UniProtDrug::parseCSVRecord)
            .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType,uniProtDrug));
    }

    public static void importData() {
      Stopwatch sw = Stopwatch.createStarted();
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
          .ifPresent(new DrugUniprotInfoConsumer(RelTypes.DRUG_TARGET));
      // Drug Enzyme
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
          .ifPresent(new DrugUniprotInfoConsumer(RelTypes.DRUG_ENZYME));
      // Drug Transporter
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
          .ifPresent(new DrugUniprotInfoConsumer(RelTypes.DRUG_TRANSPORTER));
      //Drug Carrier
      FrameworkPropertyService.INSTANCE
          .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
          .ifPresent(new DrugUniprotInfoConsumer(RelTypes.DRUG_CARRIER));
      AsyncLoggingService.logInfo("drug data import completed: "
      +sw.elapsed(TimeUnit.SECONDS) +" seconds");
    }

  private static void testImportData() {
  // use generic TestGraphDataConsumer to test
  // Drug target
  FrameworkPropertyService.INSTANCE
      .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
      .ifPresent(path->
          new TestGraphDataConsumer().accept(path,new DrugUniprotInfoConsumer(RelTypes.DRUG_TARGET)));
  // Drug Enzyme
  FrameworkPropertyService.INSTANCE
      .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
      .ifPresent(path->
          new TestGraphDataConsumer().accept(path,new DrugUniprotInfoConsumer(RelTypes.DRUG_ENZYME)));
  // Drug Transporter
  FrameworkPropertyService.INSTANCE
      .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
      .ifPresent(path->
          new TestGraphDataConsumer().accept(path,new DrugUniprotInfoConsumer(RelTypes.DRUG_TRANSPORTER)));
  //Drug Carrier
  FrameworkPropertyService.INSTANCE
      .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
      .ifPresent(path->
          new TestGraphDataConsumer().accept(path,new DrugUniprotInfoConsumer(RelTypes.DRUG_CARRIER)));
}
    public static void main(String[] args) {
      DrugUniprotInfoConsumer.testImportData();

    }

    }

