package org.nygenome.als.graphdb.consumer;

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.EmbeddedGraph.RelTypes;
import org.nygenome.als.graphdb.util.AsyncLoggingService;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.UniProtDrug;
import java.nio.file.Path;
import scala.Tuple2;

/*
A Consumer responsible
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer  {
    private static final Logger log = Logger.getLogger(DrugUniprotInfoConsumer.class);

    private final EmbeddedGraph.RelTypes eRelType;

    private BiConsumer<RelTypes, UniProtDrug> proteinDrugRelationshiprConsumer
        = (drugRelType, drug) -> {
        String uniprotId = drug.uniprotId();
        // check if the protein node exists
        if (proteinMap.containsKey(uniprotId)) {
            Node proteinNode = proteinMap.get(uniprotId);
            drug.drugIdList().forEach((id) -> {
                Node drugNode = (drugMap.containsKey(id)) ? drugMap.get(id)
                    : resolveDrugBankNode.apply(id);
                proteinDrugRelMap.put(new Tuple2<>(uniprotId, id),
                    proteinNode.createRelationshipTo(drugNode, drugRelType));
            });
        } else {
            AsyncLoggingService.logError("resolveProteinDrugRelationship: "
                + " uniprot id: " + uniprotId + " is not registered.");
        }
    };

    private void resolveProteinDrugRelationship(@Nonnull String uniprotId,
        @Nonnull RelTypes drugRelType, java.util.List<String> drugBankIdList) {
        Node proteinNode = resolveProteinNodeFunction.apply(uniprotId);
        drugBankIdList.forEach((id) -> {
            Node drugNode = (drugMap.containsKey(id)) ? drugMap.get(id)
                : resolveDrugBankNode.apply(id);
            proteinDrugRelMap.put(new Tuple2<>(uniprotId, id),
                proteinNode.createRelationshipTo(drugNode, drugRelType));
        });
    }

/*
Sample line from csv
    ID,Name,Gene Name,GenBank Protein ID,GenBank Gene ID,UniProt ID,Uniprot Title,PDB ID,GeneCard ID,GenAtlas ID,HGNC ID,Species,Drug IDs
P45059,Peptidoglycan synthase FtsI,ftsI,1574687,L42023,P45059,FTSI_HAEIN,"",,,,Haemophilus influenzae (strain ATCC 51907 / DSM 11121 / KW20 / Rd),DB00303
     */

    public DrugUniprotInfoConsumer(EmbeddedGraph.RelTypes eRelTypes) {
        this.eRelType = eRelTypes;
    }

    @Override
    public void accept(Path path) {
        new CsvRecordStreamSupplier(path).get()
            .map(UniProtDrug::parseCSVRecord)
            .forEach(uniProtDrug -> proteinDrugRelationshiprConsumer.accept(eRelType,uniProtDrug));

    }

    }

