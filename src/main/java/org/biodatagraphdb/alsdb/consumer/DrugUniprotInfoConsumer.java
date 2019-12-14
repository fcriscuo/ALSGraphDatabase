package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceSupplier;
import org.neo4j.graphdb.Node;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
A Consumer responsible for processing the UniProt-DrugBank files
for: drug targets, drug enzymes, drug carriers, and drug transporters
Will create new Protein nodes if uniprotId value is novel
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer {

  private org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes eRelType;

  public DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode runMode, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes eRelType) {
    super(runMode);
    this.eRelType = eRelType;
  }

  private void addDrugTypeLabel(Node node) {
    String drugInteractionType = eRelType.name();
    switch (drugInteractionType) {
      case ("DRUG_TARGET"):
        lib.novelLabelConsumer.accept(node, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Target);
        break;
      case ("DRUG_ENZYME"):
        lib.novelLabelConsumer.accept(node, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Enzyme);
        break;
      case ("DRUG_TRANSPORTER"):
        lib.novelLabelConsumer.accept(node, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Transporter);
        break;
      case ("DRUG_CARRIER"):
        lib.novelLabelConsumer.accept(node, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Carrier);
        break;
      default:
        AsyncLoggingService.logError(drugInteractionType + " is an invalid drug type");
    }
  }

  /*
  Private Consumer that will resolve the properties for a DrugBank entry
   */
  private BiConsumer<String, Node> completeDrugBankNodeProperties = (id, node) -> {
    org.biodatagraphdb.alsdb.service.DrugBankService.INSTANCE.getDrugBankValueById(id)
        .ifPresent(dbv -> {
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugId", dbv.drugBankId()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugName", dbv.drugName()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugType", dbv.drugType()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("CASNumber", dbv.casNumber()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("RxListLink", dbv.rxListLink()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("NDCLink", dbv.ndcLink()));
        });
  };


  private BiConsumer<org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes, org.biodatagraphdb.alsdb.value.UniProtDrug> proteinDrugRelationshiprConsumer
      = (drugRelType, drug) -> {
    String uniprotId = drug.uniprotId();
    // check if the protein node exists
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      // label the Protein Node with its drug characteristic
      addDrugTypeLabel(proteinNode);
      drug.drugIdList().forEach((id) -> {
        Node drugNode = resolveDrugBankNode.apply(id);
        completeDrugBankNodeProperties.accept(drug.id(), drugNode);
        lib.resolveNodeRelationshipFunction.apply(new Tuple2<>(proteinNode, drugNode),
            drugRelType);
      });

  };

/*
Sample line from csv
    ID,Name,Gene Name,GenBank Protein ID,GenBank Gene ID,UniProt ID,Uniprot Title,PDB ID,GeneCard ID,GenAtlas ID,HGNC ID,Species,Drug IDs
P45059,Peptidoglycan synthase FtsI,ftsI,1574687,L42023,P45059,FTSI_HAEIN,"",,,,Haemophilus influenzae (strain ATCC 51907 / DSM 11121 / KW20 / Rd),DB00303
     */

  @Override
  public void accept(Path path) {
    new org.biodatagraphdb.alsdb.util.CsvRecordStreamSupplier(path).get()
        .map(org.biodatagraphdb.alsdb.value.UniProtDrug::parseCSVRecord)
        .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType, uniProtDrug));
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TARGET));
    // Drug Enzyme
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_ENZYME));
    // Drug Transporter
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER));
    //Drug Carrier
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_CARRIER));
    AsyncLoggingService.logInfo("drug data import completed: "
        + sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  private static void testImportData() {
    // use generic TestGraphDataConsumer to test
    // Drug target
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TARGET)));
    // Drug Enzyme
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_ENZYME)));
    // Drug Transporter
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path,
                    new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER)));
    //Drug Carrier
    org.biodatagraphdb.alsdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes.DRUG_CARRIER)));
  }
 // test mode
  public static void main(String[] args) {
    DrugUniprotInfoConsumer.testImportData();

  }

}

