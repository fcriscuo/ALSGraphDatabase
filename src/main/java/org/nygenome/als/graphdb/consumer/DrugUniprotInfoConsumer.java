package org.nygenome.als.graphdb.consumer;

import org.apache.log4j.Logger;
import org.nygenome.als.graphdb.EmbeddedGraph;
import org.nygenome.als.graphdb.util.CsvRecordStreamSupplier;
import org.nygenome.als.graphdb.value.UniProtDrug;
import java.nio.file.Path;

/*
A Consumer responsible
 */

public class DrugUniprotInfoConsumer extends GraphDataConsumer  {
    private static final Logger log = Logger.getLogger(DrugUniprotInfoConsumer.class);

    private final EmbeddedGraph.RelTypes eRelType;

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

