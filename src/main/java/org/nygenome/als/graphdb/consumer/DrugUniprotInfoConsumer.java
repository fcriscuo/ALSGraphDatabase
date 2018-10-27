package org.nygenome.als.graphdb.consumer;

import com.google.common.base.Stopwatch;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;
import org.nygenome.als.graphdb.integration.TestGraphDataConsumer;
import org.nygenome.als.graphdb.service.DrugBankService;
import org.nygenome.als.graphdb.supplier.GraphDatabaseServiceSupplier.RunMode;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import org.nygenome.als.graphdb.value.UniProtDrug;
import scala.Tuple2;

/*
A Consumer responsible for processing the UniProt-DrugBank files
for: drug targets, drug enzymes, drug carriers, and drug transporters
Will create new Protein nodes if uniprotId value is novel
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer {

  private RelTypes eRelType;

  public DrugUniprotInfoConsumer(RunMode runMode, ALSDatabaseImportApp.RelTypes eRelType) {
    super(runMode);
    this.eRelType = eRelType;
  }

  private void addDrugTypeLabel(Node node) {
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

  /*
  Private Consumer that will resolve the properties for a DrugBank entry
   */
  private BiConsumer<String, Node> completeDrugBankNodeProperties = (id, node) -> {
    DrugBankService.INSTANCE.getDrugBankValueById(id)
        .ifPresent(dbv -> {
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugId", dbv.drugBankId()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugName", dbv.drugName()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugType", dbv.drugType()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("CASNumber", dbv.casNumber()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("RxListLink", dbv.rxListLink()));
          lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("NDCLink", dbv.ndcLink()));
        });
  };


  private BiConsumer<RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
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
    new CsvRecordStreamSupplier(path).get()
        .map(UniProtDrug::parseCSVRecord)
        .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType, uniProtDrug));
  }

  public static void importProdData() {
    Stopwatch sw = Stopwatch.createStarted();
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, RelTypes.DRUG_TARGET));
    // Drug Enzyme
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, RelTypes.DRUG_ENZYME));
    // Drug Transporter
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, RelTypes.DRUG_TRANSPORTER));
    //Drug Carrier
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, RelTypes.DRUG_CARRIER));
    AsyncLoggingService.logInfo("drug data import completed: "
        + sw.elapsed(TimeUnit.SECONDS) + " seconds");
  }

  private static void testImportData() {
    // use generic TestGraphDataConsumer to test
    // Drug target
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, RelTypes.DRUG_TARGET)));
    // Drug Enzyme
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, RelTypes.DRUG_ENZYME)));
    // Drug Transporter
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path,
                    new DrugUniprotInfoConsumer(RunMode.TEST, RelTypes.DRUG_TRANSPORTER)));
    //Drug Carrier
    FrameworkPropertyService.INSTANCE
        .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
        .ifPresent(path ->
            new TestGraphDataConsumer()
                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, RelTypes.DRUG_CARRIER)));
  }
 // test mode
  public static void main(String[] args) {
    DrugUniprotInfoConsumer.testImportData();

  }

}

