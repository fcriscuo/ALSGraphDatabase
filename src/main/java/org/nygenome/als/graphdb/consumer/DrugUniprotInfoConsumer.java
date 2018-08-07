package org.nygenome.als.graphdb.consumer;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Relationship;
import org.nygenome.als.graphdb.lib.FunctionLib;
import org.nygenome.als.graphdb.EmbeddedGraph;
import scala.Tuple2;
import java.nio.file.Path;
import java.util.Map;


public class DrugUniprotInfoConsumer extends GraphDataConsumer{
    private static final Logger log = Logger.getLogger(DrugUniprotInfoConsumer.class);

    private final EmbeddedGraph.RelTypes eRelType;
    //  DRUG_TARGET_UNIRPOT_FILE = /data/als/drug_target_uniprot_links.csv
    /*

    ID,Name,Gene Name,GenBank Protein ID,GenBank Gene ID,UniProt ID,Uniprot Title,PDB ID,GeneCard ID,GenAtlas ID,HGNC ID,Species,Drug IDs
P45059,Peptidoglycan synthase FtsI,ftsI,1574687,L42023,P45059,FTSI_HAEIN,"",,,,Haemophilus influenzae (strain ATCC 51907 / DSM 11121 / KW20 / Rd),DB00303
     */

    public DrugUniprotInfoConsumer(EmbeddedGraph.RelTypes eRelTypes) {
        this.eRelType = eRelTypes;
    }

    @Override
    public void accept(Path path) {
        FunctionLib.generateLineStreamFromPath(path)
                .skip(1L)   // skip the header
                .map((s) ->s.replace("\"", "").toUpperCase().trim())
                .forEach(this::processDrugUniprotInfo);
    }

    private void processDrugUniprotInfo(String line) {
        String[] tokens = line.split(COMMA_DELIM);
        String szDrugId = tokens[0];
        String szDrugType = "";

        if (line.contains("BIOTECHDRUG")) {
            szDrugType = "BIOTECHDRUG";
        } else if (line.contains("SMALLMOLECULEDRUG")) {
            szDrugType = "SMALLMOLECULEDRUG";
        } else if (line.contains("RECOMBINANT")) {
            szDrugType = "RECOMBINANT";
        } else {
            szDrugType = "ISOPHANE";
        }

        int posDrugType = line.indexOf(szDrugType);

        String szDrugName = line.substring(szDrugId.length() + 1,
                posDrugType - 1);
        String szProteinInfo = line.substring(
                posDrugType + szDrugType.length() + 1, line.length());
        int posCommaProteinInfoSplit = szProteinInfo.indexOf(",");
        String szUniprotId = szProteinInfo.substring(0,
                posCommaProteinInfoSplit);
        String szProteinName = szProteinInfo
                .substring(posCommaProteinInfoSplit + 1);
        boolean bFoundProtein = proteintMap.containsKey(szUniprotId);
        boolean bFoundDrug = drugMap.containsKey(szDrugId);
        boolean bFoundRel = false;

        Tuple2 stringTuple2 = new Tuple2<>(szUniprotId, szDrugId);

        if (bFoundProtein && bFoundDrug) {
            for (Map.Entry<Tuple2<String,String>, Relationship> eRelationship : vDrugRelMap
                    .entrySet()) {
                if (eRelationship.getKey().equals(stringTuple2)) {
                    if (eRelationship.getValue().getType().name()
                            .equals(eRelType.toString())) {
                        bFoundRel = true;
                        break;
                    }
                }
            }
        }
        if (false == bFoundRel) {
            if (!bFoundProtein) {
                createProteinNode(strNoInfo, szUniprotId, strNoInfo,
                        szProteinName, strNoInfo, strNoInfo);
            }
            if (!bFoundDrug) {
                createDrugNode(szDrugId, szDrugName, szDrugType);
            }

            vDrugRelMap.put(
                    stringTuple2,
                    proteintMap.get(szUniprotId).createRelationshipTo(
                            drugMap.get(szDrugId), eRelType));

        }
    }
    }

