package proteinFramework;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import org.nygenome.als.graphdb.app.ALSDatabaseImportApp;
import org.nygenome.als.graphdb.util.Utils;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.LabelTypes;
import org.nygenome.als.graphdb.app.ALSDatabaseImportApp.RelTypes;


public class ProteinNetworkOrig {
    private final String GENE_DISEASE_ASSOC_DISGENET_FILE = "files/curated_gene_disease_associations_DisGeNet.txt";
    private final String GENE_UNIPROT_ID_ASSOC_DISGENET_FILE = "files/gene_uniprotId_mapping_DisGeNet.txt";
    private final String UNIPROT_REACTOME_HOMOSAPIENS_MAPPING = "files/UniProt2Reactome_HomoSapiens.txt";
    private final String KANEKO_UNIPROT_DISEASE_ASSOC_FILE = "files/cleannames_Kaneko.txt";
    private final String PPI_INTACT_FILE = "files/intact_output_protein_interactions.txt";
    private final String UNIRPOT_IDS_ENSEMBL = "files/Ensembl_Uniprot.txt";
    private final String HUMAN_TISSUE_ATLAS = "files/HumanTissueAtlas.txt";
    private final String DRUG_TARGET_UNIRPOT_FILE = "files/drug_target_uniprot_links.csv";
    private final String DRUG_ENZYME_UNIRPOT_FILE = "files/drug_enzyme_uniprot_links.csv";
    private final String DRUG_TRANSPORTER_UNIRPOT_FILE = "files/drug_transporter_uniprot_links.csv";
    private final String DRUG_CARRIER_UNIRPOT_FILE = "files/drug_carrier_uniprot_links.csv";
    private final String SEQ_SIM_FILE = "files/uniprot_07_04_2015_blast.tab.txt";
    // GEO Study files
    private final String GSE43696_GEO_STUDY = "files/GEOStudyFiles/diff_expr_GSE43696.txt";
    private final String GSE27876_GEO_STUDY = "files/GEOStudyFiles/diff_expr_GSE27876.txt";
    private final String GSE63142_GEO_STUDY = "files/GEOStudyFiles/diff_expr_GSE63142.txt";

    private final String ALL_UNIRPOT = "files/GEOStudyFiles/uniprot_all.xls";
    private final String UNIRPOT_GSE55962 = "files/GEOStudyFiles/uniprot_GSE55962.xls";

    public static enum EnsemblType_e {
        ENST, ENSG
    }

    public enum Resource_t {
        eKaneko, eDisGeNet, ePPI
    }

    Map<String, Node> proteintMap = new HashMap<String, Node>();
    Map<String, Node> diseaseMap = new HashMap<String, Node>();
    Map<String, Node> drugMap = new HashMap<String, Node>();
    Map<String, Node> pathwayMap = new HashMap<String, Node>();
    Map<String, Node> tissueMap = new HashMap<String, Node>();
    Map<String, Node> GEOStudyMap = new HashMap<String, Node>();
    Map<StringPair, Node> GEOComparisonMap = new HashMap<StringPair, Node>();
    Map<StringPair, Relationship> vDiseaseRelMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vKanekoRelMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vDrugRelMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vTissueRelMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vPPIMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vPathwayMap = new HashMap<StringPair, Relationship>();
    Map<StringTriplet, Relationship> vGEOGeneRelMap = new HashMap<StringTriplet, Relationship>();
    Map<StringPair, Relationship> vGEOComponentsRelMap = new HashMap<StringPair, Relationship>();
    Map<StringPair, Relationship> vSeqSimMap = new HashMap<StringPair, Relationship>();
    String strApostrophe = "\"";
    private String strNoInfo = "NA";

    public void readDataFromDisGeNETFile() throws IOException {
        loadGeneUniprotIdAssociationInfo(GENE_UNIPROT_ID_ASSOC_DISGENET_FILE);
        readGeneDiseaseAssociationInfo(GENE_DISEASE_ASSOC_DISGENET_FILE);
    }

    public void readDrugInfo() throws IOException {
        readDrugUniprotIdAssociationInfo(DRUG_TARGET_UNIRPOT_FILE,
                RelTypes.DRUG_TARGET);
        readDrugUniprotIdAssociationInfo(DRUG_ENZYME_UNIRPOT_FILE,
                RelTypes.DRUG_ENZYME);
        readDrugUniprotIdAssociationInfo(DRUG_TRANSPORTER_UNIRPOT_FILE,
                RelTypes.DRUG_TRANSPORTER);
        readDrugUniprotIdAssociationInfo(DRUG_CARRIER_UNIRPOT_FILE,
                RelTypes.DRUG_CARRIER);
    }

    public void readHumanTissueAtlasInfo() throws IOException {
        readUniprotIds_Ensembl(UNIRPOT_IDS_ENSEMBL);
        readTissueInfo(HUMAN_TISSUE_ATLAS);
        createEnsemblTissueAssociation(HUMAN_TISSUE_ATLAS);
    }

    private void readGeneDiseaseAssociationInfo(String szInputFileName)
            throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                String szUniprotId = null;
                for (Entry<String, Node> eProtein : proteintMap.entrySet()) {
                    if (eProtein.getValue().getProperty("ProteinId").toString()
                            .equals(tokens[0])) {
                        szUniprotId = eProtein.getKey();
                        proteintMap.get(szUniprotId).setProperty("GeneSymbol",
                                tokens[1]);
                        break;
                    }
                }

                if (null != szUniprotId) {
                    if (!diseaseMap.containsKey(tokens[4])) {
                        createDiseaseNode(tokens[4]);
                    }

                    StringPair strPair = new StringPair(szUniprotId, tokens[4]);
                    if (!vDiseaseRelMap.containsKey(strPair)) {
                        vDiseaseRelMap
                                .put(strPair,
                                        proteintMap
                                                .get(szUniprotId)
                                                .createRelationshipTo(
                                                        diseaseMap
                                                                .get(tokens[4]),
                                                        Utils.convertStringToRelType(tokens[7])));
                        vDiseaseRelMap.get(strPair).setProperty(
                                "Confidence_level",
                                Double.parseDouble(tokens[5]));
                        vDiseaseRelMap.get(strPair).setProperty("Reference",
                                tokens[8]);
                    }
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void loadGeneUniprotIdAssociationInfo(String szInputFileName)
            throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                if (!proteintMap.containsKey(tokens[3])) {
                    createProteinNode(tokens[0], tokens[3], strNoInfo,
                            tokens[4], strNoInfo, strNoInfo);
                } else {
                    proteintMap.get(tokens[3]).setProperty("ProteinId",
                            tokens[0]);
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void readPathwayInfo() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                UNIPROT_REACTOME_HOMOSAPIENS_MAPPING))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                boolean bFoundPathway = pathwayMap.containsKey(tokens[1]);
                StringPair strPair = new StringPair(tokens[0], tokens[1]);
                if (!vPathwayMap.containsKey(strPair)) {
                    if (!proteintMap.containsKey(tokens[0])) {
                        createProteinNode(strNoInfo, tokens[0], strNoInfo,
                                strNoInfo, strNoInfo, strNoInfo);
                    }

                    if (!bFoundPathway) {
                        createPathwayNode(tokens[1], tokens[3]);
                    }

                    vPathwayMap.put(
                            strPair,
                            proteintMap.get(tokens[0]).createRelationshipTo(
                                    pathwayMap.get(tokens[1]),
                                    RelTypes.IN_PATHWAY));
                    vPathwayMap.get(strPair).setProperty("Reference",
                            "Reactome");
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readDataFromKanekoPaper() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                KANEKO_UNIPROT_DISEASE_ASSOC_FILE))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                if (!tokens[3].toUpperCase().trim().equals("NA")) {
                    for (int i = 0; i < tokens.length; i++) {
                        tokens[i] = tokens[i].toUpperCase().trim();
                    }

                    StringPair strPair = new StringPair(tokens[3], tokens[1]);
                    if (!vKanekoRelMap.containsKey(strPair)) {
                        if (!proteintMap.containsKey(tokens[3])) {
                            createProteinNode(strNoInfo, tokens[3], strNoInfo,
                                    strNoInfo, tokens[2], strNoInfo);
                        } else {
                            proteintMap.get(tokens[3]).setProperty(
                                    "GeneSymbol", tokens[2]);
                        }

                        if (!diseaseMap.containsKey(tokens[1])) {
                            createDiseaseNode(tokens[1]);
                        }

                        vKanekoRelMap.put(
                                strPair,
                                proteintMap.get(tokens[3])
                                        .createRelationshipTo(
                                                diseaseMap.get(tokens[1]),
                                                RelTypes.KANEKO_ASSOCIATED));
                        vKanekoRelMap.get(strPair).setProperty("Reference",
                                "Kaneko_etAll_2013");
                    }
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readDataFromIntact() throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                PPI_INTACT_FILE))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                if (!tokens[0].equals(tokens[1])) {
                    StringPair strPair = new StringPair(tokens[0], tokens[1]);
                    StringPair strReversePair = new StringPair(tokens[1],
                            tokens[0]);

                    if ((!vPPIMap.containsKey(strPair))
                            && (!vPPIMap.containsKey(strReversePair))) {

                        if (!proteintMap.containsKey(tokens[0])) {
                            createProteinNode(strNoInfo, tokens[0], strNoInfo,
                                    strNoInfo, strNoInfo, strNoInfo);
                        }

                        if (!proteintMap.containsKey(tokens[1])) {
                            createProteinNode(strNoInfo, tokens[1], strNoInfo,
                                    strNoInfo, strNoInfo, strNoInfo);
                        }

                        vPPIMap.put(
                                strPair,
                                proteintMap
                                        .get(tokens[0])
                                        .createRelationshipTo(
                                                proteintMap.get(tokens[1]),
                                                Utils.convertStringToRelType(tokens[5])));
                        vPPIMap.get(strPair).setProperty(
                                "Interaction_method_detection", tokens[2]);
                        vPPIMap.get(strPair)
                                .setProperty("Reference", tokens[3]);
                        vPPIMap.get(strPair).setProperty(
                                "Publication_identifier", tokens[4]);

                        int len = tokens[6].length();
                        int istart = 0;
                        if (len >= 4) {
                            istart = len - 4;
                        } else {
                            istart = 0;
                        }
                        String strToken = tokens[6].substring(istart, len);

                        if (strToken.contains("\"")) {
                            strToken = strToken.replace("\"", "");
                        }

                        if (strToken.startsWith(".")) {
                            strToken = "0" + strToken;
                        }

                        vPPIMap.get(strPair).setProperty("Confidence_level",
                                Double.parseDouble(strToken));
                    }
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void readUniprotIds_Ensembl(String szInputFileName)
            throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                if (!proteintMap.containsKey(tokens[0])) {
                    createProteinNode(strNoInfo, tokens[0], strNoInfo,
                            strNoInfo, strNoInfo, strNoInfo);
                }

                proteintMap.get(tokens[0]).setProperty("EnsemblTranscript",
                        tokens[1]);
                proteintMap.get(tokens[0])
                        .setProperty("ProteinName", tokens[2]);
                proteintMap.get(tokens[0]).setProperty("GeneSymbol", tokens[3]);
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void readTissueInfo(String szInputFileName) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {

            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String newLine = line.replaceAll(strApostrophe, "");
                String[] tokens = newLine.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                String[] szTissueTokens = tokens[2].split("[:;]");
                for (int i = 0; i < szTissueTokens.length; i = i + 2) {
                    if (!tissueMap.containsKey(szTissueTokens[i])) {
                        createTissueNode(szTissueTokens[i]);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void createEnsemblTissueAssociation(String szInputFileName)
            throws IOException {

        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {

            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String newLine = line.replaceAll(strApostrophe, "");
                String[] tokens = newLine.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                String[] szTissueTokens = tokens[2].split("[:;]");

                for (Entry<String, Node> eProtein : proteintMap.entrySet()) {
                    if (eProtein.getValue().getProperty("EnsemblTranscript")
                            .equals(tokens[1])) {
                        eProtein.getValue()
                                .setProperty("GeneSymbol", tokens[0]);

                        for (int i = 0; i < szTissueTokens.length; i = i + 2) {
                            for (Entry<String, Node> eTissue : tissueMap
                                    .entrySet()) {
                                if (eTissue.getKey().equals(szTissueTokens[i])) {
                                    StringPair strPair = new StringPair(
                                            tokens[1], eTissue.getKey());
                                    if (!vTissueRelMap.containsKey(strPair)) {
                                        vTissueRelMap
                                                .put(strPair,
                                                        eProtein.getValue()
                                                                .createRelationshipTo(
                                                                        eTissue.getValue(),
                                                                        RelTypes.TISSUE_ENHANCED));
                                        vTissueRelMap.get(strPair).setProperty(
                                                "RNA_TS_FPKM_value",
                                                szTissueTokens[i + 1]);
                                    }
                                    break;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    private void readDrugUniprotIdAssociationInfo(String szInputFileName,
                                                  RelTypes eRelType) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szInputFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.

                line = line.replace("\"", "").toUpperCase().trim();
                String delims = "[,]";
                String[] tokens = line.split(delims);

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

                StringPair strPair = new StringPair(szUniprotId, szDrugId);

                if (bFoundProtein && bFoundDrug) {
                    for (Entry<StringPair, Relationship> eRelationship : vDrugRelMap
                            .entrySet()) {
                        if (eRelationship.getKey().equals(strPair)) {
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
                            strPair,
                            proteintMap.get(szUniprotId).createRelationshipTo(
                                    drugMap.get(szDrugId), eRelType));

                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readGEOStudyInfo() throws FileNotFoundException, IOException {

        List<String> lofGEOComparisonSet_GSE43696 = new ArrayList<String>();
        List<String> lofGEOComparisonSet_GSE27876 = new ArrayList<String>();
        List<String> lofGEOComparisonSet_GSE63142 = new ArrayList<String>();

        mapENSEMBL_ID_to_UNIPROT_ID(ALL_UNIRPOT, EnsemblType_e.ENST);
        mapENSEMBL_ID_to_UNIPROT_ID(UNIRPOT_GSE55962, EnsemblType_e.ENSG);

        lofGEOComparisonSet_GSE43696.add("NC-MMA");
        lofGEOComparisonSet_GSE43696.add("NC-SA");
        lofGEOComparisonSet_GSE43696.add("MMA-SA");
        importGEOStudyInfo(GSE43696_GEO_STUDY, "GSE43696",
                lofGEOComparisonSet_GSE43696, EnsemblType_e.ENST);

        lofGEOComparisonSet_GSE27876.add("MiA-SA");
        lofGEOComparisonSet_GSE27876.add("NC-MiA");
        lofGEOComparisonSet_GSE27876.add("NC-SA");
        importGEOStudyInfo(GSE27876_GEO_STUDY, "GSE27876",
                lofGEOComparisonSet_GSE27876, EnsemblType_e.ENST);

        lofGEOComparisonSet_GSE63142.add("NC-MMA");
        lofGEOComparisonSet_GSE63142.add("NC-SA");
        lofGEOComparisonSet_GSE63142.add("MMA-SA");
        importGEOStudyInfo(GSE63142_GEO_STUDY, "GSE63142",
                lofGEOComparisonSet_GSE63142, EnsemblType_e.ENST);
    }

    private void importGEOStudyInfo(String szGEOStudyFileName,
                                    String szGEOStudyName, List<String> listOfGEOComp,
                                    EnsemblType_e eEnsemblType) throws IOException {

        if (!GEOStudyMap.containsKey(szGEOStudyName)) {
            createGEOStudyNode(szGEOStudyName);
        }

        for (int i = 0; i < listOfGEOComp.size(); i++) {
            StringPair szPair = new StringPair(listOfGEOComp.get(i),
                    szGEOStudyName);
            createGEOComparisonNode(szPair);

            vGEOComponentsRelMap.put(
                    szPair,
                    GEOComparisonMap.get(szPair).createRelationshipTo(
                            GEOStudyMap.get(szGEOStudyName), RelTypes.PART_OF));
        }

        try (BufferedReader br = new BufferedReader(new FileReader(
                szGEOStudyFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                for (Entry<String, Node> eProtein : proteintMap.entrySet()) {

                    String szValue = strNoInfo;
                    if (EnsemblType_e.ENST == eEnsemblType) {
                        szValue = eProtein.getValue()
                                .getProperty("EnsemblTranscript").toString();
                    } else {
                        szValue = eProtein.getValue().getProperty("EnsemblId")
                                .toString();
                    }
                    if (szValue.contains(tokens[0])) {

                        for (int i = 0; i < listOfGEOComp.size(); i++) {
                            StringPair strPair = new StringPair(
                                    listOfGEOComp.get(i), szGEOStudyName);
                            StringTriplet strTriplet = new StringTriplet(
                                    eProtein.getKey(),
                                    strPair.getFirstString(),
                                    strPair.getSecondString());

                            double fAdjVal = Math.round(Double
                                    .parseDouble(tokens[i + 1]) * 1000.0) / 1000.0;
                            if (vGEOGeneRelMap.containsKey(strTriplet)) {

                                if (Double.parseDouble(vGEOGeneRelMap
                                        .get(strTriplet).getProperty("AdjPVal")
                                        .toString()) > fAdjVal) {
                                    vGEOGeneRelMap.get(strTriplet).setProperty(
                                            "AdjPVal", fAdjVal);
                                }
                            } else {
                                vGEOGeneRelMap
                                        .put(strTriplet,
                                                proteintMap
                                                        .get(eProtein.getKey())
                                                        .createRelationshipTo(
                                                                GEOComparisonMap
                                                                        .get(strPair),
                                                                RelTypes.DEG_RELATED_TO));
                                vGEOGeneRelMap.get(strTriplet).setProperty(
                                        "AdjPVal", fAdjVal);
                            }
                        }
                    }
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void mapENSEMBL_ID_to_UNIPROT_ID(String szUniprotFileName,
                                             EnsemblType_e eEnsemblType) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(
                szUniprotFileName))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                if (!proteintMap.containsKey(tokens[0])) {
                    createProteinNode(strNoInfo, tokens[0], strNoInfo,
                            strNoInfo, strNoInfo, strNoInfo);
                }

                proteintMap.get(tokens[0])
                        .setProperty("ProteinName", tokens[1]);
                proteintMap.get(tokens[0]).setProperty("GeneSymbol", tokens[2]);

                if (EnsemblType_e.ENST == eEnsemblType) {
                    proteintMap.get(tokens[0]).setProperty("EnsemblTranscript",
                            tokens[3]);
                } else {
                    proteintMap.get(tokens[0]).setProperty("EnsemblId",
                            tokens[3]);
                }
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void addSeqSimilarityInfo() throws IOException {
        try (BufferedReader br = new BufferedReader(
                new FileReader(SEQ_SIM_FILE))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                String delims = "[\t]";
                String[] tokens = line.split(delims);

                for (int i = 0; i < tokens.length; i++) {
                    tokens[i] = tokens[i].toUpperCase().trim();
                }

                if (!tokens[1].equals(tokens[4])) {
                    StringPair strPair = new StringPair(tokens[1], tokens[4]);
                    StringPair strReversePair = new StringPair(tokens[4],
                            tokens[1]);

                    if (proteintMap.containsKey(tokens[1])
                            && proteintMap.containsKey(tokens[4])) {
                        if ((!vSeqSimMap.containsKey(strPair))
                                && (!vSeqSimMap.containsKey(strReversePair))
                                && (1 == Integer.parseInt(tokens[10]))) {
                            vSeqSimMap.put(
                                    strPair,
                                    proteintMap.get(tokens[1])
                                            .createRelationshipTo(
                                                    proteintMap.get(tokens[4]),
                                                    RelTypes.SEQ_SIM));

                            vSeqSimMap
                                    .get(strPair)
                                    .setProperty(
                                            "Similarity_Score",
                                            Math.round(Double
                                                    .parseDouble(tokens[7]) * 1000.0) / 1000.0);
                            vSeqSimMap.get(strPair).setProperty(
                                    "Similarity_Significance", tokens[8]);
                            vSeqSimMap
                                    .get(strPair)
                                    .setProperty(
                                            "Alignment_Length",
                                            Math.round(Double
                                                    .parseDouble(tokens[9]) * 1000.0) / 1000.0);
                        }
                    }
                }
            }
        }
        catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    private void createProteinNode(String strProteinId, String szUniprotId,
                                   String szEnsembleTranscript, String szProteinName,
                                   String szGeneSymbol, String szEnsembl) {
        proteintMap.put(szUniprotId, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.Protein));
        proteintMap.get(szUniprotId).setProperty("ProteinId", strProteinId);
        proteintMap.get(szUniprotId).setProperty("UniprotId", szUniprotId);
        proteintMap.get(szUniprotId).setProperty("EnsemblTranscript",
                szEnsembleTranscript);
        proteintMap.get(szUniprotId).setProperty("EnsemblId", szEnsembl);
        proteintMap.get(szUniprotId).setProperty("ProteinName", szProteinName);
        proteintMap.get(szUniprotId).setProperty("GeneSymbol", szGeneSymbol);
    }

    private void createDiseaseNode(String szDiseaseName) {
        diseaseMap.put(szDiseaseName, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.Disease));
        diseaseMap.get(szDiseaseName).setProperty("DiseaseName", szDiseaseName);
    }

    private void createPathwayNode(String szPathwayId, String szPathwayName) {
        pathwayMap.put(szPathwayId, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.Pathway));
        pathwayMap.get(szPathwayId).setProperty("PathwayName", szPathwayName);
    }

    private void createDrugNode(String szDrugId, String szDrugName,
                                String szDrugType) {
        drugMap.put(szDrugId,
                ALSDatabaseImportApp.getGraphInstance().createNode(LabelTypes.Drug));
        drugMap.get(szDrugId).setProperty("DrugId", szDrugId);
        drugMap.get(szDrugId).setProperty("DrugName", szDrugName);
        drugMap.get(szDrugId).setProperty("DrugType", szDrugType);
    }

    private void createGEOStudyNode(String szGEOStudyName) {
        GEOStudyMap.put(szGEOStudyName, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.GEOStudy));
        GEOStudyMap.get(szGEOStudyName).setProperty("GEOStudyID",
                szGEOStudyName);
    }

    private void createGEOComparisonNode(StringPair szPair) {
        GEOComparisonMap.put(szPair, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.GEOComparison));
        GEOComparisonMap.get(szPair).setProperty("GEOComparisonID",
                szPair.getFirstString());
    }

    private void createTissueNode(String szTissueName) {
        tissueMap.put(szTissueName, ALSDatabaseImportApp.getGraphInstance()
                .createNode(LabelTypes.Tissue));
        tissueMap.get(szTissueName).setProperty("TissueName", szTissueName);
    }
}
