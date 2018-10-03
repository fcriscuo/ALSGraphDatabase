package org.nygenome.als.graphdb;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.nygenome.als.graphdb.consumer.*;
import org.nygenome.als.graphdb.app.EmbeddedGraphApp.RelTypes;
import org.nygenome.als.graphdb.util.FrameworkPropertyService;
import proteinFramework.ProteinNetworkOrig;

public class AlsNetwork {

	public static enum EnsemblType_e {
		ENST, ENSG
	}

	public enum Resource_t {
		eKaneko, eDisGeNet, ePPI
	}

	public void readDataFromDisGeNETFile() throws IOException {
//		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_UNIPROT_ID_ASSOC_DISGENET_FILE")
//				.ifPresent(new GeneUniprotIdAssociationDataConsumer());
//		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("GENE_DISEASE_ASSOC_DISGENET_FILE")
//				.ifPresent(new GeneUniprotIdAssociationDataConsumer());

	}

	public void readDrugInfo() {
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("DRUG_TARGET_UNIRPOT_FILE")
				.ifPresent( (path) -> new DrugUniprotInfoConsumer(RelTypes.DRUG_TARGET).accept(path));
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("DRUG_ENZYME_UNIRPOT_FILE")
				.ifPresent( (path) -> new DrugUniprotInfoConsumer(RelTypes.DRUG_ENZYME).accept(path));
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("DRUG_TRANSPORTER_UNIRPOT_FILE")
				.ifPresent( (path) -> new DrugUniprotInfoConsumer(RelTypes.DRUG_TRANSPORTER).accept(path));
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("DRUG_CARRIER_UNIRPOT_FILE")
				.ifPresent( (path) -> new DrugUniprotInfoConsumer(RelTypes.DRUG_CARRIER).accept(path));
	}

	public void readHumanTissueAtlasInfo() throws IOException {
//		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIRPOT_IDS_ENSEMBL")
//				.ifPresent(new UniprotIdEnsemblDataConsumer());
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("HUMAN_TISSUE_ATLAS")
				.ifPresent(new HumanTissueAtlasDataConsumer());
	}

	public void readPathwayInfo() throws IOException {
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("UNIPROT_REACTOME_HOMOSAPIENS_MAPPING")
				.ifPresent(new PathwayInfoConsumer());

	}

	public void readDataFromKanekoPaper() throws IOException {

	}

	public void readDataFromIntact() throws IOException {
		FrameworkPropertyService.INSTANCE.getOptionalPathProperty("PPI_INTACT_DIR")
				.ifPresent(new IntactDataConsumer());
	}



	public void readGEOStudyInfo() throws FileNotFoundException, IOException {


	}

	private void mapENSEMBL_ID_to_UNIPROT_ID(String szUniprotFileName,
											 ProteinNetworkOrig.EnsemblType_e eEnsemblType) throws IOException {
	}

	public void addSeqSimilarityInfo() throws IOException {}

}
