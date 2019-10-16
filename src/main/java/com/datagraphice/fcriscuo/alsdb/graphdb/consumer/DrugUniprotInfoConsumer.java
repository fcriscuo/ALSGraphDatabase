package com.datagraphice.fcriscuo.alsdb.graphdb.consumer;

import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.datagraphice.fcriscuo.alsdb.graphdb.integration.TestGraphDataConsumer;
import com.datagraphice.fcriscuo.alsdb.graphdb.supplier.GraphDatabaseServiceSupplier;
import edu.jhu.fcriscu1.als.graphdb.value.UniProtDrug;
import org.neo4j.graphdb.Node;
import edu.jhu.fcriscu1.als.graphdb.util.AsyncLoggingService;
import scala.Tuple2;

/*
A Consumer responsible for processing the UniProt-DrugBank files
for: drug targets, drug enzymes, drug carriers, and drug transporters
Will create new Protein nodes if uniprotId value is novel
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer {

  private com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes eRelType;

  public DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode runMode, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes eRelType) {
    super(runMode);
    this.eRelType = eRelType;
  }

  private void addDrugTypeLabel(Node node) {
    String drugInteractionType = eRelType.name();
    switch (drugInteractionType) {
      case ("DRUG_TARGET"):
        lib.getNovelLabelConsumer().accept(node, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Target);
        break;
      case ("DRUG_ENZYME"):
        lib.getNovelLabelConsumer().accept(node, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Enzyme);
        break;
      case ("DRUG_TRANSPORTER"):
        lib.getNovelLabelConsumer().accept(node, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Transporter);
        break;
      case ("DRUG_CARRIER"):
        lib.getNovelLabelConsumer().accept(node, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.LabelTypes.Drug_Carrier);
        break;
      default:
        AsyncLoggingService.logError(drugInteractionType + " is an invalid drug type");
    }
  }

  /*
  Private Consumer that will resolve the properties for a DrugBank entry
   */
  private BiConsumer<String, Node> completeDrugBankNodeProperties = (id, node) -> {
    com.datagraphice.fcriscuo.alsdb.graphdb.service.DrugBankService.INSTANCE.getDrugBankValueById(id)
        .ifPresent(dbv -> {
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("DrugId", dbv.drugBankId()));
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("DrugName", dbv.drugName()));
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("DrugType", dbv.drugType()));
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("CASNumber", dbv.casNumber()));
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("RxListLink", dbv.rxListLink()));
          lib.getNodePropertyValueConsumer().accept(node, new Tuple2<>("NDCLink", dbv.ndcLink()));
        });
  };


  private BiConsumer<com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
      = (drugRelType, drug) -> {
    String uniprotId = drug.uniprotId();
    // check if the protein node exists
      Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
      // label the Protein Node with its drug characteristic
      addDrugTypeLabel(proteinNode);
      drug.drugIdList().forEach((id) -> {
        Node drugNode = resolveDrugBankNode.apply(id);
        completeDrugBankNodeProperties.accept(drug.id(), drugNode);
        lib.getResolveNodeRelationshipFunction().apply(new Tuple2<>(proteinNode, drugNode),
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
    new com.datagraphice.fcriscuo.alsdb.graphdb.util.CsvRecordStreamSupplier(path).get()
        .map(UniProtDrug::parseCSVRecord)
        .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType, uniProtDrug));
    lib.shutDown();
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TARGET));
    // Drug Enzyme
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_ENZYME));
    // Drug Transporter
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER));
    //Drug Carrier
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.PROD, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_CARRIER));
    AsyncLoggingService.logInfo("drug data import completed: "
        + sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  private static void testImportData() {
    // use generic TestGraphDataConsumer to test
    // Drug target
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TARGET)));
    // Drug Enzyme
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_ENZYME)));
    // Drug Transporter
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path,
                    new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER)));
    //Drug Carrier
    com.datagraphice.fcriscuo.alsdb.graphdb.util.FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(GraphDatabaseServiceSupplier.RunMode.TEST, com.datagraphice.fcriscuo.alsdb.graphdb.app.ALSDatabaseImportApp.RelTypes.DRUG_CARRIER)));
  }
 // test mode
  public static void main(String[] args) {
    DrugUniprotInfoConsumer.testImportData();

  }

}

