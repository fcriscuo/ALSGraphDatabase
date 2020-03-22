package org.biodatagraphdb.alsdb.consumer;

import com.google.common.base.Stopwatch;
import org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp;
import org.biodatagraphdb.alsdb.integration.TestGraphDataConsumer;
import org.biodatagraphdb.alsdb.model.UniProtDrug;
import org.biodatagraphdb.alsdb.service.graphdb.RunMode;
import org.biodatagraphdb.alsdb.supplier.GraphDatabaseServiceLegacySupplier;
import org.biodatagraphdb.alsdb.util.AsyncLoggingService;
import org.biodatagraphdb.alsdb.util.FrameworkPropertyService;
import org.neo4j.graphdb.Node;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/*
A Consumer responsible for processing the UniProt-DrugBank files
for: drug targets, drug enzymes, drug carriers, and drug transporters
Will create new Protein nodes if uniprotId value is novel
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer {

    private ALSDatabaseImportApp.RelTypes eRelType;

    public DrugUniprotInfoConsumer(RunMode runMode, org.biodatagraphdb.alsdb.app.ALSDatabaseImportApp.RelTypes eRelType) {
        super(runMode);
        this.eRelType = eRelType;
    }

    private void addDrugTypeLabel(Node node) {
        String drugInteractionType = eRelType.name();
        switch (drugInteractionType) {
            case ("DRUG_TARGET"):
                lib.novelLabelConsumer.accept(node, ALSDatabaseImportApp.LabelTypes.Drug_Target);
                break;
            case ("DRUG_ENZYME"):
                lib.novelLabelConsumer.accept(node, ALSDatabaseImportApp.LabelTypes.Drug_Enzyme);
                break;
            case ("DRUG_TRANSPORTER"):
                lib.novelLabelConsumer.accept(node, ALSDatabaseImportApp.LabelTypes.Drug_Transporter);
                break;
            case ("DRUG_CARRIER"):
                lib.novelLabelConsumer.accept(node, ALSDatabaseImportApp.LabelTypes.Drug_Carrier);
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
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugId", dbv.getDrugBankId()));
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugName", dbv.getDrugName()));
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("DrugType", dbv.getDrugType()));
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("CASNumber", dbv.getCasNumber()));
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("RxListLink", dbv.getRxListLink()));
                    lib.nodePropertyValueConsumer.accept(node, new Tuple2<>("NDCLink", dbv.getNdcLink()));
                });
    };

    private BiConsumer<ALSDatabaseImportApp.RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
            = (drugRelType, drug) -> {
        String uniprotId = drug.getUniprotId();
        // check if the protein node exists
        Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
        // label the Protein Node with its drug characteristic
        addDrugTypeLabel(proteinNode);
        drug.getDrugIdList().forEach((id) -> {
            Node drugNode = resolveDrugBankNode.apply(id);
            completeDrugBankNodeProperties.accept(drug.getId(), drugNode);
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
                .map(UniProtDrug.Companion::parseCSVRecord)
                .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType, uniProtDrug));
        lib.shutDown();
    }

    public static void importProdData() {
        Stopwatch sw = Stopwatch.createStarted();
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
                .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, ALSDatabaseImportApp.RelTypes.DRUG_TARGET));
        // Drug Enzyme
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
                .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, ALSDatabaseImportApp.RelTypes.DRUG_ENZYME));
        // Drug Transporter
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
                .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER));
        //Drug Carrier
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
                .ifPresent(new DrugUniprotInfoConsumer(RunMode.PROD, ALSDatabaseImportApp.RelTypes.DRUG_CARRIER));
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
                                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, ALSDatabaseImportApp.RelTypes.DRUG_TARGET)));
        // Drug Enzyme
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
                .ifPresent(path ->
                        new TestGraphDataConsumer()
                                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, ALSDatabaseImportApp.RelTypes.DRUG_ENZYME)));
        // Drug Transporter
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
                .ifPresent(path ->
                        new TestGraphDataConsumer()
                                .accept(path,
                                        new DrugUniprotInfoConsumer(RunMode.TEST, ALSDatabaseImportApp.RelTypes.DRUG_TRANSPORTER)));
        //Drug Carrier
        FrameworkPropertyService.INSTANCE
                .getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
                .ifPresent(path ->
                        new TestGraphDataConsumer()
                                .accept(path, new DrugUniprotInfoConsumer(RunMode.TEST, ALSDatabaseImportApp.RelTypes.DRUG_CARRIER)));
    }

    // test mode
    public static void main(String[] args) {
        DrugUniprotInfoConsumer.testImportData();

    }

}

